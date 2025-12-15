# vintagestory-restic

A containerized Vintage Story dedicated server with automated, deduplication-optimized backups using Restic.

## Overview

Run a Vintage Story dedicated server with built-in backup automation. The backup system converts the game's SQLite-based savegame format into a directory structure designed to help Restic's deduplication algorithm, which should help reduce the size of diff-based backups.

> [!WARNING]  
> This project is experimental and should be properly vetted if you want to use this on a high-stakes server where not losing your savefile is paramount.

## Features

- **Automatic Server Binary Management**: Download/update server binaries from a configurable URL with automatic version checking
- **Restic Backups**: De-duplicated backups using custom format converts `.vcdbs` savegames into a structure that maximizes Restic deduplication efficiency
- **Incremental Backup Caching**: Preserve file metadata minimizing Restic diff operations
- **Player-Aware Backup Scheduling**: Optionally pause backups when no players are online.
- **Graceful Process Management**: Proper signal handling with configurable graceful shutdown timeouts
- **Non-Root Container**: Natively non-root container implementation

## Quick Start

```yaml
services:
  vintagestory:
    image: ghcr.io/renorris/vintagestory-restic:latest
    restart: unless-stopped
    environment:
      # Required: URL to Vintage Story server archive
      VS_SERVER_TARGZ_URL: "https://cdn.vintagestory.at/gamefiles/stable/vs_server_linux-x64_1.21.6.tar.gz"

      # Backup configuration (optional - omit BACKUP_INTERVAL to disable backups)
      BACKUP_INTERVAL: "1h"
      RESTIC_REPOSITORY: "s3:s3.amazonaws.com/bucket/path"
      RESTIC_PASSWORD: "your-restic-password"
      DO_BACKUP_ON_SERVER_START: "true"
      BACKUP_PAUSE_WHEN_NO_PLAYERS: "true"

      # Set according to your Restic configuration. See https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html
      # AWS_ACCESS_KEY_ID: ""
      # AWS_SECRET_ACCESS_KEY: ""
      # etc.
    ports:
      - 42420:42420/tcp
      - 42420:42420/udp
    volumes:
      - ./gamedata:/gamedata
      - serverbinaries:/serverbinaries
      - backupcache:/backupcache

volumes:
  serverbinaries:
  backupcache:
```

## Configuration

### Required Environment Variables

| Variable | Description |
|----------|-------------|
| `VS_SERVER_TARGZ_URL` | URL to the Vintage Story server `.tar.gz` archive. Please use a URL from https://account.vintagestory.at/ (Show all available downloads and mirrors of Vintage Story -> [Linux tar.gz Archive (server only)]) |

### Backup Environment Variables

| Variable | Description |
|----------|-------------|
| `BACKUP_INTERVAL` | Backup frequency (e.g., `30m`, `1h`, `6h`). If unset, backups are disabled. |
| `RESTIC_REPOSITORY` | Restic repository location (required if backups enabled) |
| `RESTIC_PASSWORD` | Restic repository password (required if backups enabled) |
| `DO_BACKUP_ON_SERVER_START` | If `true`, triggers a backup immediately when the server boots |
| `BACKUP_PAUSE_WHEN_NO_PLAYERS` | If `true`, skips backups when no players are online |

Additional Restic backend credentials (e.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `B2_ACCOUNT_ID`) should be set according to your storage backend. See https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html

### Volume Mounts

| Path | Description |
|------|-------------|
| `/gamedata` | Game data directory containing saves, configs, logs, and player data |
| `/serverbinaries` | Server binary installation directory (managed automatically, cached for reuse across boots) |
| `/backupcache` | Persistent staging directory for backup operations |

## Architecture

### Launcher

The launcher binary orchestrates the entire server lifecycle:

1. **Binary Download**: Checks for server updates and downloads new versions when available
2. **Server Process Management**: Fork-execs the Vintage Story server, managing stdin/stdout pipes for command I/O
3. **Backup Scheduling**: Runs periodic backups at the configured interval
4. **Signal Handling**: Propagates SIGINT/SIGTERM for graceful shutdown

### vcdbtree Format

The vcdbtree format enables efficient deduplication. Vintage Story stores world data in SQLite databases (`.vcdbs` files), which have non-deterministic serialization that makes deduplication algorithms in restic very inefficient. The vcdbtree format addresses this by:

**Extracting BLOBs to Individual Files**: Each chunk, mapchunk, and mapregion is written as a separate binary file.

**Geographic Sharding**: Position-based tables use a two-level directory structure based on chunk coordinates extracted from the 64-bit ChunkPos value:

```
chunks/
  <chunkZ>/
    <chunkX>/
      <position_hex>.bin
```

**Directory Structure**:

```
staging/
  Saves/
    <worldname>/
      chunks/           # World chunk data (sharded by coordinates)
      mapchunks/        # Map chunk data (sharded by coordinates)
      mapregions/       # Map region data (sharded by coordinates)
      gamedata/         # Game state data (flat)
      playerdata/       # Player data (flat)
  Logs/                 # Server logs
  Playerdata/           # Player files
  Mods/                 # Installed mods
  serverconfig.json
  servermagicnumbers.json
```

## CLI Tools

### vcdbtree

A standalone utility for converting between `.vcdbs` and vcdbtree formats:

**Installation:**

```bash
# Install directly (requires Go 1.25+ and GCC for CGO)
CGO_ENABLED=1 go install github.com/renorris/vintagestory-restic/cmd/vcdbtree@latest
```

**Usage:**

```bash
# Convert a savegame to vcdbtree format
vcdbtree split /gamedata/Backups/backup.vcdbs /tmp/backup-tree

# Reconstruct a savegame from vcdbtree format
vcdbtree combine /tmp/backup-tree /gamedata/Saves/restored.vcdbs
```

This tool is for manually inspecting or restoring backups.

## License

MIT License. See [LICENSE](LICENSE) for details.
