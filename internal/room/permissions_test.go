package room

import (
	"encoding/json"
	"testing"

	"github.com/team4tune/node-server/internal/protocol"
)

func TestHostAssignedToCreator(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")

	if !rm.IsHost("a") {
		t.Fatal("creator should be host")
	}
	if rm.IsHost("b") {
		t.Fatal("second joiner should not be host")
	}
}

func TestHostSuccessionToOldest(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	c := newClient("c")
	rm.Join(a, "")
	rm.Join(b, "")
	rm.Join(c, "")

	rm.HardLeave(reg, "a")

	if !rm.IsHost("b") {
		t.Fatal("oldest remaining member should become host")
	}
	if rm.IsHost("c") {
		t.Fatal("newer member should not be host after succession")
	}
}

func TestPolicyEnforcement(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")

	if !rm.IsAllowed("b", protocol.ScopeEnqueue) {
		t.Fatal("default policy everyone should allow non-host")
	}

	rm.SetSettings(protocol.RoomSettings{
		Enqueue: protocol.PolicyHost,
		Skip:    protocol.PolicyEveryone,
		Remove:  protocol.PolicyEveryone,
		Control: protocol.PolicyEveryone,
	})

	if rm.IsAllowed("b", protocol.ScopeEnqueue) {
		t.Fatal("host-only enqueue should reject non-host")
	}
	if !rm.IsAllowed("a", protocol.ScopeEnqueue) {
		t.Fatal("host-only enqueue should allow host")
	}
}

func startedRoom(t *testing.T) (*Registry, *Room, *Client) {
	t.Helper()
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))
	waitType(t, a, protocol.TypePrepare)
	rm.startPlayback("t1")
	waitType(t, a, protocol.TypeNowPlaying)
	return reg, rm, a
}

func TestPauseFreezesPosition(t *testing.T) {
	_, rm, a := startedRoom(t)

	rm.Pause()
	env := waitType(t, a, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if !np.Paused {
		t.Fatal("expected paused now_playing after Pause")
	}

	rm.Resume()
	env = waitType(t, a, protocol.TypeNowPlaying)
	_ = json.Unmarshal(env.Data, &np)
	if np.Paused {
		t.Fatal("expected unpaused now_playing after Resume")
	}
	if np.T0 <= nowMs() {
		t.Fatal("resume t0 should be in the future")
	}
}

func TestSeekUpdatesPosition(t *testing.T) {
	_, rm, a := startedRoom(t)

	rm.SeekTo(50000)
	env := waitType(t, a, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if np.S != 50000 {
		t.Fatalf("expected seek s=50000, got %d", np.S)
	}
}

func TestSkipAdvancesQueue(t *testing.T) {
	_, rm, a := startedRoom(t)
	rm.AddTrack(readyTrack("t2"))

	rm.Skip()

	for {
		env := waitType(t, a, protocol.TypePrepare)
		var p protocol.PrepareData
		_ = json.Unmarshal(env.Data, &p)
		if p.TrackID == "t2" {
			return
		}
	}
}

func TestCatchUpReflectsPaused(t *testing.T) {
	reg, rm, _ := startedRoom(t)
	rm.Pause()

	b := newClient("b")
	rm.Join(b, "")
	_ = reg

	env := waitType(t, b, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if !np.Paused {
		t.Fatal("late joiner should see paused state via catchUp")
	}
}
