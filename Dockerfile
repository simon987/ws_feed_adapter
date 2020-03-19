# Build
FROM golang:1.14 as go_build
WORKDIR /build/

COPY main.go .
COPY go.mod .
RUN GOOS=linux CGO_ENABLED=0 go build -a -installsuffix cgo -o ws_feed_adapter .

FROM scratch

WORKDIR /root/
COPY --from=go_build ["/build/ws_feed_adapter", "/root/"]

CMD ["/root/ws_feed_adapter"]
