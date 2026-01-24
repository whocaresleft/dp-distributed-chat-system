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

RUN go build -o /node ./cmd/app/main.go
RUN go build -o /bootstrap ./cmd/bootstrap/main.go
RUN go build -o /nameserevr ./cmd/nameserevr/main.go

CMD ["./node"]