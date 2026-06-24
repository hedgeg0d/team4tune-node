package room

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

func waitStreamBitrate(t *testing.T, c *Client, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case env := <-c.Send:
			if env.Type != protocol.TypeRoomState {
				continue
			}
			var st protocol.RoomStateData
			_ = json.Unmarshal(env.Data, &st)
			if st.Settings.StreamBitrateKbps == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for streamBitrateKbps=%d", want)
		}
	}
}

func TestStreamBitrateClamp(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeStream)
	a := newClient("a")
	rm.Join(a, "")

	rm.SetSettings(protocol.RoomSettings{Sync: protocol.SyncResponsive, StreamBitrateKbps: 9999})
	waitStreamBitrate(t, a, protocol.StreamBitrateMaxKbps)

	rm.SetSettings(protocol.RoomSettings{Sync: protocol.SyncResponsive, StreamBitrateKbps: 1})
	waitStreamBitrate(t, a, protocol.StreamBitrateMinKbps)
}

func TestStreamModeNoSignalScheduling(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeStream)
	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))

	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case env := <-a.Send:
			if env.Type == protocol.TypePrepare || env.Type == protocol.TypeNowPlaying {
				t.Fatalf("stream mode should not emit %q", env.Type)
			}
		case <-deadline:
			return
		}
	}
}
