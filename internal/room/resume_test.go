package room

import (
	"encoding/json"
	"testing"

	"github.com/team4tune/node-server/internal/protocol"
)

func tokenFor(t *testing.T, c *Client) string {
	t.Helper()
	env := waitType(t, c, protocol.TypeRoomState)
	var rs protocol.RoomStateData
	_ = json.Unmarshal(env.Data, &rs)
	if rs.ResumeToken == "" {
		t.Fatal("expected a resume token in room_state")
	}
	return rs.ResumeToken
}

func TestResumeReattachesSameIdentityAndHost(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	token := tokenFor(t, a)
	rm.Join(b, "")

	if !rm.IsHost("a") {
		t.Fatal("a should be host")
	}

	rm.Leave(reg, a)
	if !rm.IsHost("a") {
		t.Fatal("host role should be held during the resume grace window")
	}

	back := newClient("ignored-id")
	rm.Join(back, token)
	if back.ID != "a" {
		t.Fatalf("resume should restore client id a, got %q", back.ID)
	}
	if !rm.IsHost("a") {
		t.Fatal("a should still be host after resume")
	}
}

func TestPendingMemberRemainsVisibleDuringResumeGrace(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")

	rm.Leave(reg, a)

	env := waitType(t, b, protocol.TypeRoomState)
	var rs protocol.RoomStateData
	_ = json.Unmarshal(env.Data, &rs)
	for _, m := range rs.Members {
		if m.ID == "a" {
			return
		}
	}
	t.Fatalf("pending member a is not visible: %+v", rs.Members)
}

func TestFinalizeLeavePromotesAfterGrace(t *testing.T) {
	reg := NewRegistry()
	rm := reg.Create(protocol.ModeSignal)
	a := newClient("a")
	b := newClient("b")
	rm.Join(a, "")
	rm.Join(b, "")

	rm.Leave(reg, a)
	rm.finalizeLeave(reg, "a")

	if !rm.IsHost("b") {
		t.Fatal("after grace expiry the oldest remaining member should be host")
	}
}
