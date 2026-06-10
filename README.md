# team4tune-node-server

Go backend for [team4tune](https://github.com/team4tune) — synchronized group music
listening. A "node" coordinates rooms, queues, the NTP-style clock sync, and
(M1+) the yt-dlp media pipeline.

Anonymous and open source by design: no accounts, no PII, no analytics. Rooms are
ephemeral and vanish when empty.

## Status

- M0 — WebSocket rooms in memory: create/join, live member list, clock ping-pong.
- M1 — source pipeline: `enqueue` URL → yt-dlp + ffmpeg → Opus cache (dedup by URL hash) → `/media` served with HTTP Range. Queue updates pushed in `room_state`.
- M3 (server) — playback state machine: pick a ready track → broadcast `prepare` → collect `ready` (quorum or timeout) → `now_playing { t0, s }` → track-end timer advances the queue. Late joiners catch up via `prepare`/`now_playing` on join.

## Run

```sh
go run ./cmd/server
# listens on :8080 (override with TEAM4TUNE_ADDR)
```

WebSocket endpoint: `ws://<host>:8080/ws`. Health check: `GET /healthz`.

## Test

```sh
go test ./...
```

## Layout

```
cmd/server        entrypoint
internal/protocol wire format (see docs/protocol.md)
internal/room     in-memory room registry + playback state machine
internal/media    yt-dlp + ffmpeg pipeline, Opus cache
internal/wsapi    websocket handler
```

The network e2e test is gated: `TEAM4TUNE_E2E=1 go test ./internal/wsapi/ -run EndToEnd`.

## Config

| env | default | meaning |
|-----|---------|---------|
| `TEAM4TUNE_ADDR` | `:8080` | listen address |
| `TEAM4TUNE_BASE_URL` | `http://localhost:8080` | base URL for `fileUrl` in tracks |
| `TEAM4TUNE_CACHE_DIR` | `./cache` | Opus cache directory |

## Roadmap

- M2 — refine clock sync (dedicated UDP endpoint)
- M3 — signal-mode scheduled playback
- M4 — drift correction, late join
- Later — stream mode, local-file upload, Tor/P2P
