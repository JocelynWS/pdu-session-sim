# Stage 1: Build the Go binaries
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build all 4 Network Functions
RUN CGO_ENABLED=0 GOOS=linux go build -o amf-nf cmd/amf/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o smf-nf cmd/smf/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o udm-nf cmd/udm/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o upf-nf cmd/upf/main.go

# Stage 2: Final lightweight image
FROM alpine:latest

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/amf-nf /app/amf
COPY --from=builder /app/smf-nf /app/smf
COPY --from=builder /app/udm-nf /app/udm
COPY --from=builder /app/upf-nf /app/upf

# Copy config and static web assets
COPY --from=builder /app/config.yaml /app/config.yaml
COPY --from=builder /app/web /app/web

# Expose ports
EXPOSE 8080 8081 8082 8083 8805/udp

# Default command (can be overridden by compose)
CMD ["/app/smf"]
