FROM golang:1.14.2-stretch AS builder

ENV CGO_ENABLED=0

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build
RUN go test -v ./...

RUN touch config.yaml

FROM alpine:3.11

WORKDIR /root

COPY --from=builder /build/cassandra-data-apis .
COPY --from=builder /build/config.yaml .

CMD [ "/root/cassandra-data-apis", "--config", "/root/config.yaml" ]
