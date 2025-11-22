FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -o events-service .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/events-service .

RUN chmod +x ./events-service

EXPOSE 8082

CMD ["./events-service"]