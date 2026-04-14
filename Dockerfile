# ─── Stage 1: build ───────────────────────────────────────────────────────────
# --platform=$BUILDPLATFORM ensures the correct Go binary is used for the target architecture in multi-platform builds.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

# Build args injected by Docker Buildx for multi-platform builds
ARG TARGETOS
ARG TARGETARCH

# ca-certificates needed for TLS to external registries during `go mod download`
RUN apk add --no-cache ca-certificates git

WORKDIR /src

# Download dependencies first (cached layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a fully static binary
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /jaiscloud ./cmd/jaiscloud/

# ─── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM scratch

# TLS roots so HTTPS calls (e.g. Prometheus scrape) work
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# The binary is the only thing in the image
COPY --from=builder /jaiscloud /jaiscloud

# Default port
EXPOSE 4566

# Prometheus metrics port (same port, /metrics path — no second port needed)

ENTRYPOINT ["/jaiscloud"]
CMD ["start"]
