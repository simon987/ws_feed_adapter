package main

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/urfave/cli"
	"log"
	"net/http"
	"os"
	"sync/atomic"
)

type ConnCtx struct {
	Exchange string   `json:"exchange"`
	Topics   []string `json:"topics"`
}

var serverCtx struct {
	Connections  int32
	rabbitmqHost string
}

func main() {
	logrus.SetLevel(logrus.TraceLevel)

	app := cli.NewApp()
	app.Name = "ws_feed_adapter"
	app.Usage = "Expose RabbitMQ feed to websocket"
	app.Email = "me@simon987.net"
	app.Author = "simon987"
	app.Version = "1.0"

	var listenAddr string

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "rabbitmq_host",
			Usage:       "RabbitMQ Host",
			Destination: &serverCtx.rabbitmqHost,
			Value:       "amqp://guest:guest@localhost:5672/",
		},
		cli.StringFlag{
			Name:        "listen, l",
			Usage:       "Listen address",
			Destination: &listenAddr,
			Value:       "localhost:3090",
		},
	}

	app.Action = func(c *cli.Context) error {

		serverCtx.Connections = 0

		logrus.WithField("listen", listenAddr).Info("Listening for incoming connections")
		http.HandleFunc("/socket", serveWs)
		err := http.ListenAndServe(listenAddr, nil)

		return err
	}

	err := app.Run(os.Args)
	if err != nil {
		logrus.Fatal(err)
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func serveWs(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logrus.WithError(err).Error("upgrade")
		return
	}

	logrus.WithFields(logrus.Fields{
		"remote": c.RemoteAddr().String(),
		"num":    serverCtx.Connections,
	}).Info("New connection")
	atomic.AddInt32(&serverCtx.Connections, 1)

	defer c.Close()
	defer atomic.AddInt32(&serverCtx.Connections, -1)

	mt, message, err := c.ReadMessage()
	if err != nil {
		log.Println("read:", err)
		return
	}

	logrus.WithFields(logrus.Fields{
		"mt":      mt,
		"message": string(message),
		"remote":  c.RemoteAddr().String(),
	}).Debug("Received message")

	var ctx ConnCtx
	err = json.Unmarshal(message, &ctx)
	if err != nil {
		logrus.WithError(err).Error("unmarshal")
		return
	}

	err = c.WriteMessage(websocket.TextMessage, []byte("{\"msg\": \"acknowledged, starting write loop.\"}"))
	if err != nil {
		logrus.WithError(err).Error("write")
		return
	}

	err = consumeRabbitmqMessage(&ctx, func(msg amqp.Delivery) error {
		err := c.WriteMessage(websocket.TextMessage, msg.Body)
		if err != nil {
			logrus.WithError(err).Error("write delivery")
		}
		return err
	})
	logrus.WithError(err).Error("consume")
}

func consumeRabbitmqMessage(ctx *ConnCtx, consume func(amqp.Delivery) error) error {

	logrus.Info()
	conn, err := amqp.Dial(serverCtx.rabbitmqHost)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		ctx.Exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for _, topic := range ctx.Topics {
		err = ch.QueueBind(
			q.Name,
			topic,
			ctx.Exchange,
			false,
			nil)
		if err != nil {
			return err
		}
		logrus.WithField("topic", topic).Debug("Bound topic")
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for d := range msgs {
		err := consume(d)
		if err != nil {
			return err
		}
	}
	return nil
}
