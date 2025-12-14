FROM golang:1.25-alpine AS launcher-builder

# Install build dependencies for CGO (required by go-sqlite3)
RUN apk add --no-cache gcc musl-dev

WORKDIR /build

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies with module cache
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy src
COPY . .

# Build with CGO enabled (required for go-sqlite3)
# Use static linking to avoid musl libc dependency in Debian runtime image
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -ldflags '-linkmode external -extldflags "-static"' -o vintagestory-launcher ./cmd/launcher

# Build vcdbtree CLI utility
# Use static linking to avoid musl libc dependency in Debian runtime image
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -ldflags '-linkmode external -extldflags "-static"' -o vcdbtree ./cmd/vcdbtree

# Fetch restic (/usr/bin/restic)
FROM restic/restic:latest AS restic-fetcher

FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim

# Copy restic from builder
COPY --from=restic-fetcher /usr/bin/restic /usr/bin/restic

# Note: sqlite3 CLI no longer needed - vcdbtree handles database conversion natively

# Config nonroot user
RUN mkdir /gamedata /serverbinaries && \
    groupadd -g 2001 vsgroup && \
    useradd -u 2001 -g vsgroup -s /bin/false vsuser && \
    chown -R vsuser:vsgroup /gamedata /serverbinaries

# Copy launcher and vcdbtree binaries
COPY --chown=vsuser:vsgroup --from=launcher-builder /build/vintagestory-launcher /usr/local/bin/
COPY --chown=vsuser:vsgroup --from=launcher-builder /build/vcdbtree /usr/local/bin/

# Switch to the non-root user
USER vsuser

# Define the command to run the application
CMD ["/usr/local/bin/vintagestory-launcher"]
