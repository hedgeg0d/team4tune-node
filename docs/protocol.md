# team4tune wire protocol

Transport: WebSocket at `/ws`. Every message is a JSON envelope:

```json
{ "type": "<name>", "data": { ... } }
```

This document is the source of truth shared by `team4tune-node-server` and
`team4tune-client`. Keep both sides in sync.

## Client → server

| type | data | notes |
|------|------|-------|
| `create` | `{ mode, nick }` | `mode`: `signal` \| `stream`. Creates a room, auto-joins; creator becomes host. |
| `join` | `{ roomCode, nick, resumeToken? }` | Join a room. With a valid `resumeToken`, reattach the prior identity/host. |
| `enqueue` | `{ sourceUrl }` | Add a track. Gated by `enqueue` policy. `sourceUrl` is a normal media URL (resolved via yt-dlp) or an `upload://<id>` token from `POST /upload`. |
| `skip` | `{}` | Skip the current track. Gated by `skip` policy. |
| `remove` | `{ trackId }` | Remove a queued track (not the current one). Gated by `remove` policy. |
| `control` | `{ action, seekMs? }` | `action`: `pause` \| `resume` \| `seek` (`seekMs` for seek). Gated by `control` policy. |
| `set_settings` | `{ enqueue, skip, remove, control, sync }` | Replace room settings. **Host only.** |
| `ready` | `{ trackId }` | Signal mode: this client can start (buffer lead reached / downloaded). |
| `progress` | `{ trackId, haveMs, totalMs, bps, ready, confidence }` | Download/playback telemetry (~1/s). Drives adaptive start + member health. |
| `bye` | `{}` | Intentional leave: immediate departure (no resume grace, frees host now). |
| `ping` | `{ t0 }` | Clock sync. `t0` = client send time (unix ms). Also sent over UDP. |

## Server → client

| type | data | notes |
|------|------|-------|
| `room_state` | `{ roomCode, mode, selfId, hostId, settings, members[], queue[], udpPort, resumeToken }` | Full snapshot. `members[i].health = { confidence, bufferedMs, bps, ready }`. `resumeToken` is self-only. |
| `now_playing` | `{ trackId, t0, s, paused }` | Scheduled playback command; may be **unicast** to resync a late/returning client. |
| `prepare` | `{ trackId, fileUrl, durationMs, title }` | Signal mode: download this file. |
| `pong` | `{ t0, t1, t2 }` | Clock sync reply (WS or UDP). |
| `error` | `{ code, message }` | `code: "forbidden"` when a gated action is denied. |

## Permissions & host

`settings` is `{ enqueue, skip, remove, control, sync }`. The four scopes are each a
**policy** of `"everyone"` or `"host"` (default `everyone`). `sync` is the start policy
(below).

The **host** is the room creator, identified by an ephemeral session id (`hostId`); no
accounts, no credential. On disconnect the host slot is held for a resume grace window
(~60 s); if it expires the **oldest remaining** member becomes host. The client is host
when `selfId == hostId`. A gated action from a non-permitted client returns
`error{ code: "forbidden" }` and changes nothing.

## Adaptive start, telemetry & resync

Clients stream `progress` while downloading and playing. The server starts a track when
ready clients meet the deadline for the room's `sync` mode — `responsive` (short cap,
default) or `tight` (long cap) — so one slow peer never blocks the room. A client that
becomes ready **after** start (slow download, late joiner, or reconnect) is sent a unicast
`now_playing` at the live position to catch up. Per-member `health.confidence` (0–3) is
surfaced to everyone so the room can see who is struggling.

## Reconnect & resume

Each member receives a self-only `resumeToken` in `room_state`. On an unexpected socket
drop the server keeps the slot (and host role) for ~60 s; the client auto-reconnects and
`join`s with the token to reattach the same identity, seq, and host role, then resyncs via
`now_playing`. An explicit `bye` skips the grace window. Tokens are random, in-memory, and
expire — anonymity is preserved.

## Clock sync (NTP-style)

1. Client sends `ping{ t0 }` (over a dedicated **UDP** endpoint when available — its port
   is advertised as `udpPort` in `room_state` — otherwise over the WebSocket).
2. Server replies `pong{ t0, t1, t2 }` where `t1` = server receive, `t2` = server send.
3. Client records `t3` = receive time, then:

```
offset = ((t1 - t0) + (t2 - t3)) / 2
rtt    = (t3 - t0) - (t2 - t1)
```

The client bursts pings on join for fast convergence, rejects high-RTT outliers, and fits
a line `offset(t) = a + b·t` over low-RTT samples to track clock **skew**, not just a fixed
offset. UDP avoids TCP head-of-line jitter; the client falls back to WS if UDP is blocked.
Server clock is the single source of truth for scheduled playback.

## Scheduled playback (M3)

Server holds `{ trackId, t0 (serverTime), s (seekMs), paused }`.
Playback position at server time `t`: `pos = s + (t - t0)` while playing, or `s` while
paused. Client converts to local clock: `localStart = t0 - offset`.

Control actions are server-authoritative: a client sends `control`, the server mutates
the single source of truth and broadcasts a fresh `now_playing` to everyone, so the room
stays in lockstep. `pause` freezes `s` at the current position; `resume` sets a new
future `t0`; `seek` sets `s` (and a new `t0` unless paused).

## HTTP endpoints

Alongside the WebSocket, the server exposes plain HTTP:

- `GET /media/<id>.opus` — serves a transcoded track. Supports Range (`206`).
- `POST /upload` — `multipart/form-data` with field `file` (an **Ogg/Opus** file) and
  optional `title`. The client encodes the Opus locally (no server transcode); the server
  stores it under a content hash, verifies the codec is Opus, probes duration, and returns
  `{ sourceUrl: "upload://<id>", title, durationMs }`. The client then sends a normal
  `enqueue` with that `sourceUrl`. Errors: `415` non-Opus, `413` too large (100 MB cap),
  `400` missing file, `405` non-POST. The endpoint is room-agnostic; enqueue permission is
  still enforced by the `enqueue` message.

## Anonymity

No accounts, no PII. Room codes are random and ephemeral; rooms are deleted when
the last member leaves. The server persists nothing about users.
