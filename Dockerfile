# syntax=docker/dockerfile:1

# ─── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Cache dependencies before copying source
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /gkit-server ./examples/server

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
# Use distroless for minimal attack surface (~2MB image)
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /gkit-server /gkit-server

# HTTP + gRPC
EXPOSE 8080 50051

USER nonroot:nonroot
ENTRYPOINT ["/gkit-server"]
