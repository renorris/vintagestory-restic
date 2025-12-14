FROM golang:1.25-alpine AS launcher-builder

WORKDIR /build

# Copy go mod and sum files
COPY go.mod ./

# Download dependencies with module cache
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy src
COPY . .

# Build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o vintagestory-launcher ./cmd/launcher

FROM alpine:3.23

# Install .NET 8 runtime
RUN apk add --no-cache aspnetcore8-runtime

# Config nonroot user
RUN mkdir /gamedata /serverbinaries && \
    addgroup -g 2001 -S vsgroup && \
    adduser -u 2001 -S -G vsgroup vsuser && \
    chown -R vsuser:vsgroup /gamedata /serverbinaries

# Copy launcher binary
COPY --chown=vsuser:vsgroup --from=launcher-builder /build/vintagestory-launcher /usr/local/bin

# Switch to the non-root user
USER vsuser

# Define the command to run the application
CMD ["vintagestory-launcher"]