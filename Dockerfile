FROM golang:1.21-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/blobscan-ipld ./cmd/blobscan-ipld


FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /bin/blobscan-ipld /usr/local/bin/blobscan-ipld

VOLUME ["/data", "/car"]

EXPOSE 8080

ENTRYPOINT ["blobscan-ipld"]
CMD ["-config", "/etc/blobscan-ipld/config.yaml", "run"]
