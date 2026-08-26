FROM golang:1.23

WORKDIR /app

COPY . .

ENV GOPROXY=off \
    GOSUMDB=off \
    CGO_ENABLED=0

RUN go build -mod=vendor -o /out/eventpush ./cmd/eventpush

EXPOSE 8080

CMD ["/out/eventpush", "-addr", ":8080"]
