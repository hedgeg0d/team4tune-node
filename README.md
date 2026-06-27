# team4tune-node-server

Go server for synchronized group music listening. Coordinates rooms, manages playback queues, handles NTP-style clock sync, and runs the yt-dlp + ffmpeg media pipeline.

Anonymous by design — no accounts, no PII, no analytics. Rooms are ephemeral and vanish when empty.

## Quick start

```sh
go run ./cmd/server
```

WebSocket at `ws://localhost:8080/ws`. Health check at `GET /healthz`.

## Test

```sh
go test ./...
```

Network e2e test (requires yt-dlp): `TEAM4TUNE_E2E=1 go test ./internal/wsapi/ -run EndToEnd`.

## Modes

- **signal** — server sends control signals and serves the media file; each client downloads and plays locally, synced to a shared wall clock (tight sync, <50 ms target).
- **stream** — server decodes the current track to Opus via WebRTC and broadcasts live to all clients. WebRTC's jitter buffer handles timing; no clock sync needed.

## Configuration

| Env | Default | Description |
|-----|---------|-------------|
| `TEAM4TUNE_ADDR` | `:8080` | Listen address |
| `TEAM4TUNE_BASE_URL` | `http://localhost:8080` | Base URL for `fileUrl` in tracks |
| `TEAM4TUNE_CACHE_DIR` | `./cache` | Opus cache directory |

## Layout

```
cmd/server        entrypoint
internal/protocol wire protocol types
internal/room     room registry and playback state machine
internal/media    yt-dlp + ffmpeg pipeline and Opus cache
internal/wsapi    WebSocket handler
internal/udpclock UDP clock sync endpoint
internal/httpapi  HTTP endpoints (upload, media serving)
internal/rtc      WebRTC broadcaster
```

## Protocol

See [docs/protocol.md](docs/protocol.md) for the wire protocol reference.
