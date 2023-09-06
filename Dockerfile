FROM golang:1.18.5 AS builder
LABEL stage=gobuilder

ENV CGO_ENABLED 0
ENV GOOS linux
ENV GOPROXY https://goproxy.cn,direct

WORKDIR /build
COPY . .
RUN ls && go mod tidy && go build -ldflags="-s -w" -o ./runnable ./cmd/main.go


FROM alpine
RUN apk update --no-cache && apk add --no-cache ca-certificates tzdata
ENV TZ Asia/Shanghai

WORKDIR /app
COPY --from=builder /build/runnable /build/etc/delayqueue.prod.yaml /app/
CMD ["./runnable", "-f", "delayqueue.prod.yaml"]