FROM golang:1.25-bookworm
RUN apt-get update && apt-get install -y \
    pkg-config \
    libzmq3-dev \
    gcc \
    libc6-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /server-app ./cmd/app/main.go

CMD ["/server-app"]