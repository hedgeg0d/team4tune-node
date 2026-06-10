package room

import (
	"encoding/json"
	"testing"

	"github.com/team4tune/node-server/internal/protocol"
)

func TestReadyDeadlineByMode(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	if rm.readyDeadline() != responsiveReadyTimeout {
		t.Fatalf("default deadline = %v, want responsive", rm.readyDeadline())
	}
	rm.SetSettings(protocol.RoomSettings{Sync: protocol.SyncTight})
	if rm.readyDeadline() != tightReadyTimeout {
		t.Fatalf("tight deadline = %v, want tight", rm.readyDeadline())
	}
}

func TestProgressReadyStartsPlayback(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))
	waitType(t, a, protocol.TypePrepare)

	rm.MarkProgress("a", protocol.ProgressData{TrackID: "t1", Ready: true})
	env := waitType(t, a, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if np.TrackID != "t1" {
		t.Fatalf("progress(ready) should start t1, got %q", np.TrackID)
	}
}

func TestLateReadyTriggersResync(t *testing.T) {
	_, rm, a := startedRoom(t)

	rm.MarkReady("a", "t1")
	env := waitType(t, a, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if np.TrackID != "t1" {
		t.Fatalf("late ready should resync now_playing, got %q", np.TrackID)
	}
}

func TestProgressUpdatesHealth(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")

	rm.MarkProgress("a", protocol.ProgressData{
		TrackID:    "t1",
		HaveMs:     1000,
		Bps:        50000,
		Confidence: 2,
	})

	for {
		env := waitType(t, a, protocol.TypeRoomState)
		var rs protocol.RoomStateData
		_ = json.Unmarshal(env.Data, &rs)
		for _, m := range rs.Members {
			if m.ID == "a" && m.Health != nil {
				if m.Health.Confidence != 2 || m.Health.Bps != 50000 {
					t.Fatalf("health = %+v", m.Health)
				}
				return
			}
		}
	}
}
