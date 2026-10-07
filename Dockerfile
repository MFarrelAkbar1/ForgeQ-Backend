# syntax=docker/dockerfile:1

# One image holds all three binaries (api, worker, migrate); compose picks one
# per service with `command`.

FROM golang:1.27-alpine AS build
WORKDIR /src

# Download modules in their own layer so code changes do not refetch them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Alpine rather than distroless so compose healthchecks can use wget.
FROM alpine:3.22
RUN addgroup -S forgeq && adduser -S -G forgeq forgeq
COPY --from=build /out/ /usr/local/bin/
USER forgeq

# Exec form: the binary is PID 1 and receives SIGTERM directly, which the
# worker needs for graceful shutdown.
CMD ["api"]
