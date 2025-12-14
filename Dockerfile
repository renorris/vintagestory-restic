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

# Fetch restic (/usr/bin/restic)
FROM restic/restic:latest AS restic-fetcher

FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim

# Copy restic from builder
COPY --from=restic-fetcher /usr/bin/restic /usr/bin/restic

# Config nonroot user
RUN mkdir /gamedata /serverbinaries && \
    groupadd -g 2001 vsgroup && \
    useradd -u 2001 -g vsgroup -s /bin/false vsuser && \
    chown -R vsuser:vsgroup /gamedata /serverbinaries

# Copy launcher binary
COPY --chown=vsuser:vsgroup --from=launcher-builder /build/vintagestory-launcher /usr/local/bin

# Switch to the non-root user
USER vsuser

# Define the command to run the application
CMD ["/usr/local/bin/vintagestory-launcher"]
