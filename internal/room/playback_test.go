package room

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

func newClient(id string) *Client {
	return &Client{ID: id, Send: make(chan protocol.Envelope, 32)}
}

func waitType(t *testing.T, c *Client, typ string) protocol.Envelope {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case env := <-c.Send:
			if env.Type == typ {
				return env
			}
		case <-deadline:
			t.Fatalf("client %s timed out waiting for %q", c.ID, typ)
		}
	}
}

func readyTrack(id string) protocol.Track {
	return protocol.Track{
		ID:         id,
		Status:     protocol.StatusReady,
		FileURL:    "http://node/media/" + id + ".opus",
		DurationMs: 300000,
	}
}

func TestPlaybackPrepareThenNowPlaying(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)

	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")

	rm.AddTrack(readyTrack("t1"))

	for _, c := range []*Client{a, b} {
		env := waitType(t, c, protocol.TypePrepare)
		var p protocol.PrepareData
		if err := json.Unmarshal(env.Data, &p); err != nil {
			t.Fatal(err)
		}
		if p.TrackID != "t1" {
			t.Fatalf("client %s prepare trackID = %q", c.ID, p.TrackID)
		}
	}

	rm.MarkReady("a", "t1")
	rm.MarkReady("b", "t1")

	for _, c := range []*Client{a, b} {
		env := waitType(t, c, protocol.TypeNowPlaying)
		var np protocol.NowPlayingData
		if err := json.Unmarshal(env.Data, &np); err != nil {
			t.Fatal(err)
		}
		if np.TrackID != "t1" {
			t.Fatalf("client %s now_playing trackID = %q", c.ID, np.TrackID)
		}
		if np.T0 <= nowMs() {
			t.Fatalf("client %s now_playing t0 not in the future: %d", c.ID, np.T0)
		}
	}
}

func TestPlaybackStartsWithoutLeftMember(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)

	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")
	rm.AddTrack(readyTrack("t1"))

	waitType(t, a, protocol.TypePrepare)
	waitType(t, b, protocol.TypePrepare)

	rm.MarkReady("a", "t1")
	rm.Leave(reg, b)

	env := waitType(t, a, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if np.TrackID != "t1" {
		t.Fatalf("expected now_playing t1 after laggard left, got %q", np.TrackID)
	}
}

func TestPlaybackTimeoutStarts(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))
	waitType(t, a, protocol.TypePrepare)

	rm.startPlayback("t1")
	env := waitType(t, a, protocol.TypeNowPlaying)
	var np protocol.NowPlayingData
	_ = json.Unmarshal(env.Data, &np)
	if np.TrackID != "t1" {
		t.Fatalf("expected now_playing t1, got %q", np.TrackID)
	}
}

func TestMarkReadyNoDuplicateUnicastAfterStart(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)

	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))

	waitType(t, a, protocol.TypePrepare)
	rm.MarkReady("a", "t1")
	waitType(t, a, protocol.TypeNowPlaying)

	for i := 0; i < 5; i++ {
		rm.MarkReady("a", "t1")
	}

	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case env := <-a.Send:
			if env.Type == protocol.TypeNowPlaying {
				t.Fatalf("got duplicate unicast now_playing after start")
			}
		case <-deadline:
			return
		}
	}
}

func TestMarkReadyUnicastOnceForLateReady(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)

	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")
	rm.AddTrack(readyTrack("t1"))

	waitType(t, a, protocol.TypePrepare)
	waitType(t, b, protocol.TypePrepare)

	rm.MarkReady("a", "t1")
	rm.startPlayback("t1")

	waitType(t, a, protocol.TypeNowPlaying)

	rm.MarkReady("b", "t1")
	firstDeadline := time.After(150 * time.Millisecond)
	seenUnicast := false
firstDrain:
	for {
		select {
		case env := <-b.Send:
			if env.Type == protocol.TypeNowPlaying {
				seenUnicast = true
			}
		case <-firstDeadline:
			break firstDrain
		}
	}
	if !seenUnicast {
		t.Fatalf("late-ready client never received its unicast now_playing")
	}

	for i := 0; i < 5; i++ {
		rm.MarkReady("b", "t1")
	}

	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case env := <-b.Send:
			if env.Type == protocol.TypeNowPlaying {
				t.Fatalf("got duplicate unicast now_playing for late-ready client")
			}
		case <-deadline:
			return
		}
	}
}

func TestPlaybackAdvancesQueueOnEnd(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")

	rm.AddTrack(readyTrack("t1"))
	rm.AddTrack(readyTrack("t2"))

	waitType(t, a, protocol.TypePrepare)
	rm.MarkReady("a", "t1")
	waitType(t, a, protocol.TypeNowPlaying)

	rm.endTrack("t1")

	env := waitType(t, a, protocol.TypePrepare)
	var p protocol.PrepareData
	_ = json.Unmarshal(env.Data, &p)
	if p.TrackID != "t2" {
		t.Fatalf("expected prepare t2 after t1 ended, got %q", p.TrackID)
	}
}

func TestPlaybackEndCleansTrack(t *testing.T) {
	cleaned := make(chan string, 1)
	reg := NewRegistry()
	reg.SetTrackCleaner(func(id string) { cleaned <- id })
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))
	waitType(t, a, protocol.TypePrepare)
	rm.MarkReady("a", "t1")
	waitType(t, a, protocol.TypeNowPlaying)

	rm.endTrack("t1")

	select {
	case id := <-cleaned:
		if id != "t1" {
			t.Fatalf("cleaned %q, want t1", id)
		}
	case <-time.After(time.Second):
		t.Fatal("track was not cleaned")
	}
}

func TestTrackCleanupWaitsForOtherRooms(t *testing.T) {
	cleaned := make(chan string, 2)
	reg := NewRegistry()
	reg.SetTrackCleaner(func(id string) { cleaned <- id })
	aRoom := reg.Create(protocol.ModeSignal)
	bRoom := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	aRoom.Join(a, "")
	bRoom.Join(b, "")
	aRoom.AddTrack(readyTrack("t1"))
	bRoom.AddTrack(readyTrack("t1"))
	waitType(t, a, protocol.TypePrepare)
	waitType(t, b, protocol.TypePrepare)
	aRoom.MarkReady("a", "t1")
	waitType(t, a, protocol.TypeNowPlaying)

	aRoom.endTrack("t1")

	select {
	case id := <-cleaned:
		t.Fatalf("cleaned %q while another room still references it", id)
	case <-time.After(100 * time.Millisecond):
	}

	bRoom.MarkReady("b", "t1")
	waitType(t, b, protocol.TypeNowPlaying)
	bRoom.endTrack("t1")

	select {
	case id := <-cleaned:
		if id != "t1" {
			t.Fatalf("cleaned %q, want t1", id)
		}
	case <-time.After(time.Second):
		t.Fatal("track was not cleaned after all rooms released it")
	}
}

func TestEmptyRoomTTLRemovesRoomAndCleansMedia(t *testing.T) {
	cleaned := make(chan string, 2)
	reg := NewRegistry()
	reg.emptyRoomTTL = 20 * time.Millisecond
	reg.SetTrackCleaner(func(id string) { cleaned <- id })
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	rm.Join(a, "")
	rm.AddTrack(readyTrack("t1"))
	waitType(t, a, protocol.TypePrepare)

	rm.Leave(reg, a)

	deadline := time.After(time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("room was not removed")
		default:
			if _, err := reg.Get(rm.Code); err == ErrRoomNotFound {
				goto removed
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

removed:
	select {
	case id := <-cleaned:
		if id != "t1" {
			t.Fatalf("cleaned %q, want t1", id)
		}
	case <-time.After(time.Second):
		t.Fatal("queued track was not cleaned")
	}
}
