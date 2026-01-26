# --- STAGE 1: BUILDER ---
FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libzmq3-dev \
    gcc \
    libc6-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/node ./cmd/node
RUN go build -o /bin/bootstrap ./cmd/bootstrap
RUN go build -o /bin/nameserver ./cmd/nameserver

# --- STAGE 2: RUNNER ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libzmq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /bin/bootstrap .
COPY --from=builder /bin/nameserver .
COPY --from=builder /bin/node .
COPY ./templates ./templates

RUN chmod +x bootstrap nameserver node

CMD ["./node"]