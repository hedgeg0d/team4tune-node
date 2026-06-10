package wsapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
	"github.com/team4tune/node-server/internal/protocol"
	"github.com/team4tune/node-server/internal/room"
)

func TestReconnectResumesIdentityOverWire(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/ws", New(room.NewRegistry(), nil))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	url := wsURL(srv)

	host := dial(t, url)
	send(t, host, protocol.TypeCreate, protocol.CreateData{Mode: protocol.ModeSignal, Nick: "host"})
	var hs protocol.RoomStateData
	_ = json.Unmarshal(readType(t, host, protocol.TypeRoomState).Data, &hs)
	if hs.ResumeToken == "" {
		t.Fatal("expected a resume token")
	}
	origID := hs.SelfID

	guest := dial(t, url)
	defer guest.Close(websocket.StatusNormalClosure, "")
	send(t, guest, protocol.TypeJoin, protocol.JoinData{RoomCode: hs.RoomCode, Nick: "guest"})
	readType(t, guest, protocol.TypeRoomState)

	host.Close(websocket.StatusAbnormalClosure, "drop")

	back := dial(t, url)
	defer back.Close(websocket.StatusNormalClosure, "")
	send(t, back, protocol.TypeJoin, protocol.JoinData{
		RoomCode:    hs.RoomCode,
		Nick:        "host",
		ResumeToken: hs.ResumeToken,
	})
	var rs protocol.RoomStateData
	_ = json.Unmarshal(readType(t, back, protocol.TypeRoomState).Data, &rs)

	if rs.SelfID != origID {
		t.Fatalf("resume should restore id %q, got %q", origID, rs.SelfID)
	}
	if rs.HostID != origID {
		t.Fatalf("resumed client should still be host, hostId=%q", rs.HostID)
	}
}
