# team4tune wire protocol

WebSocket at `/ws`. Every message is a JSON envelope:

```json
{ "type": "<name>", "data": { ... } }
```

## Client → server

| type | data | notes |
|------|------|-------|
| `create` | `{ mode, nick }` | `mode`: `signal` \| `stream`. Creates a room, auto-joins; creator becomes host. |
| `join` | `{ roomCode, nick, resumeToken? }` | Join a room. With a valid `resumeToken`, reattach the prior identity/host. |
| `enqueue` | `{ sourceUrl }` | Add a track. Gated by `enqueue` policy. `sourceUrl` is a normal media URL (resolved via yt-dlp) or an `upload://<id>` token from `POST /upload`. |
| `skip` | `{}` | Skip the current track. Gated by `skip` policy. |
| `remove` | `{ trackId }` | Remove a queued track (not the current one). Gated by `remove` policy. |
| `control` | `{ action, seekMs? }` | `action`: `pause` \| `resume` \| `seek` (`seekMs` for seek). Gated by `control` policy. |
| `set_settings` | `{ enqueue, skip, remove, control, sync, memLimitMb }` | Replace room settings. Host only. |
| `ready` | `{ trackId }` | Signal mode: this client can start (buffer lead reached / downloaded). |
| `progress` | `{ trackId, haveMs, totalMs, bps, ready, confidence }` | Download/playback telemetry (~1/s). Drives adaptive start + member health. |
| `bye` | `{}` | Intentional leave: immediate departure (no resume grace, frees host now). |
| `ping` | `{ t0 }` | Clock sync. `t0` = client send time (unix ms). Also sent over UDP. |
| `rtc` | `{ kind, sdp?, candidate? }` | WebRTC signaling for stream mode. `kind`: `join` (request broadcast) \| `answer` (sdp) \| `ice` (candidate). |

## Server → client

| type | data | notes |
|------|------|-------|
| `room_state` | `{ roomCode, mode, selfId, hostId, settings, members[], queue[], udpPort, resumeToken }` | Full snapshot. `members[i].health = { confidence, bufferedMs, bps, ready }`. `resumeToken` is self-only. |
| `now_playing` | `{ trackId, t0, s, paused }` | Scheduled playback command. May be unicast to resync a late/returning client. |
| `prepare` | `{ trackId, fileUrl, durationMs, title }` | Signal mode: download this file. |
| `pong` | `{ t0, t1, t2 }` | Clock sync reply (WS or UDP). |
| `rtc` | `{ kind, sdp?, candidate? }` | Stream mode signaling. `kind`: `offer` (sdp) \| `ice` (candidate). The server is the single WebRTC peer — it offers, the client answers. |
| `error` | `{ code, message }` | `code: "forbidden"` when a gated action is denied. |

## Permissions & host

`settings` is `{ enqueue, skip, remove, control, sync, memLimitMb, streamBitrateKbps }`.
The four scopes each have a policy of `"everyone"` or `"host"` (default `everyone`).
`sync` is the start policy. `memLimitMb` (2–100, default 50) controls the room's media
cache budget. `streamBitrateKbps` (16–256, default 64) is the Opus encode bitrate for
stream mode; changes apply to the next track started.

The host is the room creator (ephemeral session id, no accounts). On disconnect the
host slot is held for ~60 s resume grace; if it expires the oldest remaining member
becomes host. Gated actions from non-permitted clients return `error{ code: "forbidden" }`.

## Adaptive start & telemetry

Clients send `progress` while downloading/playing. The server starts a track when
ready clients meet the deadline for the room's `sync` mode — `responsive` (short cap,
default) or `tight` (long cap) — so one slow peer never blocks the room. Late clients
get a unicast `now_playing` at the live position. Per-member `health.confidence` (0–3)
is visible to everyone.

## Reconnect & resume

Each member gets a self-only `resumeToken` in `room_state`. On an unexpected socket
drop the server keeps the slot (and host role) for ~60 s. The client reconnects and
`join`s with the token to reattach the same identity, then resyncs via `now_playing`.
An explicit `bye` skips the grace window. Tokens are random, in-memory, and expire.

## Clock sync (NTP-style)

1. Client sends `ping{ t0 }` over UDP (port from `room_state.udpPort`) or WebSocket.
2. Server replies `pong{ t0, t1, t2 }` where `t1` = server receive, `t2` = server send.
3. Client records `t3` = receive time, then:

```
offset = ((t1 - t0) + (t2 - t3)) / 2
rtt    = (t3 - t0) - (t2 - t1)
```

The client bursts pings on join for fast convergence, rejects high-RTT outliers, and
fits `offset(t) = a + b·t` over low-RTT samples to track clock skew. UDP avoids TCP
head-of-line jitter; the client falls back to WS if UDP is blocked. Server clock is
the reference for scheduled playback.

## Scheduled playback (M3)

Server holds `{ trackId, t0 (serverTime), s (seekMs), paused }`.
Playback position at server time `t`: `pos = s + (t - t0)` while playing, `s` while
paused. Client converts to local clock: `localStart = t0 - offset`.

Control actions are server-authoritative: a client sends `control`, the server mutates
the state and broadcasts a fresh `now_playing` to everyone. `pause` freezes `s` at the
current position; `resume` sets a new future `t0`; `seek` sets `s` (and a new `t0`
unless paused).

## Stream mode (WebRTC broadcast)

A room with `mode: "stream"` replaces download-and-clock-sync with a live broadcast:
the server decodes the current track to Opus (at `streamBitrateKbps`) and streams it
to every client via a single WebRTC `PeerConnection`. The host drives the queue;
the server plays one track at a time and advances on track end. No
`prepare`/`now_playing`/`ready`/clock-sync — WebRTC's jitter buffer handles timing.

Signaling flow (over WebSocket):

1. Client sends `rtc{ kind: "join" }` on entering a stream room.
2. Server creates a `PeerConnection`, adds the Opus track, sends `rtc{ kind: "offer", sdp }`.
3. Client answers `rtc{ kind: "answer", sdp }`.
4. Both trickle ICE via `rtc{ kind: "ice", candidate }`.

Once connected the client plays incoming audio. `skip` cancels the current track and
advances. `control{ pause|resume }` pauses/resumes the broadcast. `seek` is not yet
supported in stream mode.

## HTTP endpoints

- `GET /media/<id>.opus` — Ogg/Opus track. Complete files support Range (`206`).
- `GET /media/<id>/index.m3u8` — HLS playlist for long remote tracks.
- `GET /media/<id>/seg/<n>.ts` — lazily generated MPEG-TS/AAC segment for index `n`.
- `POST /upload` — `multipart/form-data` with field `file` (Ogg/Opus) and optional
  `title`. Returns `{ sourceUrl: "upload://<id>", title, durationMs }`.
  Errors: `415` non-Opus, `413` too large (100 MB cap), `400` missing file, `405` non-POST.

## Anonymity

No accounts, no PII. Room codes are random and ephemeral; rooms are deleted when
the last member leaves. The server persists nothing about users.
