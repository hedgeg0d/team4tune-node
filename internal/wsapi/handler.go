package wsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/team4tune/node-server/internal/media"
	"github.com/team4tune/node-server/internal/protocol"
	"github.com/team4tune/node-server/internal/room"
)

type Handler struct {
	Registry *Registry
	Pipeline *media.Pipeline
}

type Registry = room.Registry

func New(reg *Registry, pipeline *media.Pipeline) *Handler {
	return &Handler{Registry: reg, Pipeline: pipeline}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	ctx := r.Context()
	client := &room.Client{
		ID:   uuid.NewString(),
		Send: make(chan protocol.Envelope, 32),
	}

	var current *room.Room
	defer func() {
		if current != nil {
			current.Leave(h.Registry, client)
		}
	}()

	go writePump(ctx, conn, client.Send)

	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}

		var env protocol.Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			sendErr(client, "bad_message", "invalid envelope")
			continue
		}

		switch env.Type {
		case protocol.TypeCreate:
			if current != nil {
				sendErr(client, "already_in_room", "leave current room first")
				continue
			}
			var d protocol.CreateData
			if err := json.Unmarshal(env.Data, &d); err != nil {
				sendErr(client, "bad_message", "invalid create payload")
				continue
			}
			mode := d.Mode
			if mode != protocol.ModeSignal && mode != protocol.ModeStream {
				mode = protocol.ModeSignal
			}
			client.Nick = d.Nick
			current = h.Registry.Create(mode)
			current.Join(client, "")

		case protocol.TypeJoin:
			if current != nil {
				sendErr(client, "already_in_room", "leave current room first")
				continue
			}
			var d protocol.JoinData
			if err := json.Unmarshal(env.Data, &d); err != nil {
				sendErr(client, "bad_message", "invalid join payload")
				continue
			}
			rm, err := h.Registry.Get(d.RoomCode)
			if err != nil {
				sendErr(client, "room_not_found", "no room with that code")
				continue
			}
			client.Nick = d.Nick
			current = rm
			current.Join(client, d.ResumeToken)

		case protocol.TypeEnqueue:
			if current == nil {
				sendErr(client, "not_in_room", "join a room first")
				continue
			}
			if h.Pipeline == nil {
				sendErr(client, "no_pipeline", "media pipeline unavailable")
				continue
			}
			if !current.IsAllowed(client.ID, protocol.ScopeEnqueue) {
				sendErr(client, "forbidden", "only the host can add tracks")
				continue
			}
			var d protocol.EnqueueData
			if err := json.Unmarshal(env.Data, &d); err != nil || d.SourceURL == "" {
				sendErr(client, "bad_message", "invalid enqueue payload")
				continue
			}
			rm := current
			initial := h.Pipeline.Resolve(d.SourceURL, func(t protocol.Track) {
				rm.UpsertTrack(t)
			})
			rm.UpsertTrack(initial)

		case protocol.TypeSkip:
			if current == nil {
				sendErr(client, "not_in_room", "join a room first")
				continue
			}
			if !current.IsAllowed(client.ID, protocol.ScopeSkip) {
				sendErr(client, "forbidden", "only the host can skip")
				continue
			}
			current.Skip()

		case protocol.TypeRemove:
			if current == nil {
				sendErr(client, "not_in_room", "join a room first")
				continue
			}
			if !current.IsAllowed(client.ID, protocol.ScopeRemove) {
				sendErr(client, "forbidden", "only the host can remove tracks")
				continue
			}
			var d protocol.RemoveData
			if err := json.Unmarshal(env.Data, &d); err != nil || d.TrackID == "" {
				sendErr(client, "bad_message", "invalid remove payload")
				continue
			}
			current.RemoveTrack(d.TrackID)

		case protocol.TypeControl:
			if current == nil {
				sendErr(client, "not_in_room", "join a room first")
				continue
			}
			if !current.IsAllowed(client.ID, protocol.ScopeControl) {
				sendErr(client, "forbidden", "only the host can control playback")
				continue
			}
			var d protocol.ControlData
			if err := json.Unmarshal(env.Data, &d); err != nil {
				sendErr(client, "bad_message", "invalid control payload")
				continue
			}
			switch d.Action {
			case protocol.ControlPause:
				current.Pause()
			case protocol.ControlResume:
				current.Resume()
			case protocol.ControlSeek:
				current.SeekTo(d.SeekMs)
			default:
				sendErr(client, "bad_message", "unknown control action")
			}

		case protocol.TypeSetSettings:
			if current == nil {
				sendErr(client, "not_in_room", "join a room first")
				continue
			}
			if !current.IsHost(client.ID) {
				sendErr(client, "forbidden", "only the host can change settings")
				continue
			}
			var d protocol.RoomSettings
			if err := json.Unmarshal(env.Data, &d); err != nil {
				sendErr(client, "bad_message", "invalid settings payload")
				continue
			}
			current.SetSettings(d)

		case protocol.TypeReady:
			if current == nil {
				continue
			}
			var d protocol.ReadyData
			if err := json.Unmarshal(env.Data, &d); err != nil || d.TrackID == "" {
				continue
			}
			current.MarkReady(client.ID, d.TrackID)

		case protocol.TypeProgress:
			if current == nil {
				continue
			}
			var d protocol.ProgressData
			if err := json.Unmarshal(env.Data, &d); err != nil || d.TrackID == "" {
				continue
			}
			current.MarkProgress(client.ID, d)

		case protocol.TypeBye:
			if current != nil {
				current.HardLeave(h.Registry, client.ID)
				current = nil
			}

		case protocol.TypeRTC:
			if current == nil {
				continue
			}
			var d protocol.RTCSignal
			if err := json.Unmarshal(env.Data, &d); err != nil {
				continue
			}
			current.HandleRTC(client.ID, d)

		case protocol.TypePing:
			t1 := time.Now().UnixMilli()
			var d protocol.PingData
			_ = json.Unmarshal(env.Data, &d)
			client.Send <- protocol.MustEncode(protocol.TypePong, protocol.PongData{
				T0: d.T0,
				T1: t1,
				T2: time.Now().UnixMilli(),
			})

		default:
			sendErr(client, "unknown_type", "unsupported message type")
		}
	}
}

func writePump(ctx context.Context, conn *websocket.Conn, out <-chan protocol.Envelope) {
	for {
		select {
		case <-ctx.Done():
			return
		case env, ok := <-out:
			if !ok {
				return
			}
			raw, err := json.Marshal(env)
			if err != nil {
				continue
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = conn.Write(wctx, websocket.MessageText, raw)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func sendErr(c *room.Client, code, msg string) {
	select {
	case c.Send <- protocol.MustEncode(protocol.TypeError, protocol.ErrorData{Code: code, Message: msg}):
	default:
	}
}
