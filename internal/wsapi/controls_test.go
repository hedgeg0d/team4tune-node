package wsapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
	"github.com/team4tune/node-server/internal/media"
	"github.com/team4tune/node-server/internal/protocol"
	"github.com/team4tune/node-server/internal/room"
)

func TestEnqueueForbiddenWhenLocked(t *testing.T) {
	pipeline, err := media.New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws", New(room.NewRegistry(), pipeline))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	url := wsURL(srv)

	host := dial(t, url)
	defer host.Close(websocket.StatusNormalClosure, "")
	send(t, host, protocol.TypeCreate, protocol.CreateData{Mode: protocol.ModeSignal, Nick: "host"})
	var hs protocol.RoomStateData
	_ = json.Unmarshal(readType(t, host, protocol.TypeRoomState).Data, &hs)

	send(t, host, protocol.TypeSetSettings, protocol.RoomSettings{
		Enqueue: protocol.PolicyHost,
		Skip:    protocol.PolicyEveryone,
		Remove:  protocol.PolicyEveryone,
		Control: protocol.PolicyEveryone,
	})

	guest := dial(t, url)
	defer guest.Close(websocket.StatusNormalClosure, "")
	send(t, guest, protocol.TypeJoin, protocol.JoinData{RoomCode: hs.RoomCode, Nick: "guest"})
	var gs protocol.RoomStateData
	_ = json.Unmarshal(readType(t, guest, protocol.TypeRoomState).Data, &gs)
	if gs.Settings.Enqueue != protocol.PolicyHost {
		t.Fatalf("guest should see locked enqueue policy, got %q", gs.Settings.Enqueue)
	}
	if gs.HostID == "" || gs.HostID == gs.SelfID {
		t.Fatal("guest should see a host that is not itself")
	}

	send(t, guest, protocol.TypeEnqueue, protocol.EnqueueData{SourceURL: "whatever"})
	var e protocol.ErrorData
	_ = json.Unmarshal(readType(t, guest, protocol.TypeError).Data, &e)
	if e.Code != "forbidden" {
		t.Fatalf("expected forbidden, got %q", e.Code)
	}
}

func TestSetSettingsForbiddenForGuest(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/ws", New(room.NewRegistry(), nil))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	url := wsURL(srv)

	host := dial(t, url)
	defer host.Close(websocket.StatusNormalClosure, "")
	send(t, host, protocol.TypeCreate, protocol.CreateData{Mode: protocol.ModeSignal, Nick: "host"})
	var hs protocol.RoomStateData
	_ = json.Unmarshal(readType(t, host, protocol.TypeRoomState).Data, &hs)

	guest := dial(t, url)
	defer guest.Close(websocket.StatusNormalClosure, "")
	send(t, guest, protocol.TypeJoin, protocol.JoinData{RoomCode: hs.RoomCode, Nick: "guest"})
	readType(t, guest, protocol.TypeRoomState)

	send(t, guest, protocol.TypeSetSettings, protocol.DefaultSettings())
	var e protocol.ErrorData
	_ = json.Unmarshal(readType(t, guest, protocol.TypeError).Data, &e)
	if e.Code != "forbidden" {
		t.Fatalf("expected forbidden, got %q", e.Code)
	}
}
