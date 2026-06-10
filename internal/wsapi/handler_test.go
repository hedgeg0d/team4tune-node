package wsapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/team4tune/node-server/internal/protocol"
	"github.com/team4tune/node-server/internal/room"
)

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func send(t *testing.T, conn *websocket.Conn, typ string, data any) {
	t.Helper()
	env := protocol.MustEncode(typ, data)
	raw, _ := json.Marshal(env)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readType(t *testing.T, conn *websocket.Conn, want string) protocol.Envelope {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, raw, err := conn.Read(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var env protocol.Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if env.Type == want {
			return env
		}
	}
	t.Fatalf("timed out waiting for %q", want)
	return protocol.Envelope{}
}

func wsURL(s *httptest.Server) string {
	return strings.Replace(s.URL, "http", "ws", 1) + "/ws"
}

func TestCreateJoinBroadcast(t *testing.T) {
	srv := httptest.NewServer(New(room.NewRegistry(), nil))
	defer srv.Close()
	url := wsURL(srv)

	host := dial(t, url)
	defer host.Close(websocket.StatusNormalClosure, "")
	send(t, host, protocol.TypeCreate, protocol.CreateData{Mode: protocol.ModeSignal, Nick: "host"})

	env := readType(t, host, protocol.TypeRoomState)
	var st protocol.RoomStateData
	if err := json.Unmarshal(env.Data, &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Members) != 1 || st.RoomCode == "" {
		t.Fatalf("unexpected initial state: %+v", st)
	}
	code := st.RoomCode

	guest := dial(t, url)
	defer guest.Close(websocket.StatusNormalClosure, "")
	send(t, guest, protocol.TypeJoin, protocol.JoinData{RoomCode: code, Nick: "guest"})

	genv := readType(t, guest, protocol.TypeRoomState)
	var gst protocol.RoomStateData
	if err := json.Unmarshal(genv.Data, &gst); err != nil {
		t.Fatal(err)
	}
	if len(gst.Members) != 2 {
		t.Fatalf("guest expected 2 members, got %d", len(gst.Members))
	}

	henv := readType(t, host, protocol.TypeRoomState)
	var hst protocol.RoomStateData
	if err := json.Unmarshal(henv.Data, &hst); err != nil {
		t.Fatal(err)
	}
	if len(hst.Members) != 2 {
		t.Fatalf("host expected 2 members after join, got %d", len(hst.Members))
	}
}

func TestJoinUnknownRoom(t *testing.T) {
	srv := httptest.NewServer(New(room.NewRegistry(), nil))
	defer srv.Close()

	c := dial(t, wsURL(srv))
	defer c.Close(websocket.StatusNormalClosure, "")
	send(t, c, protocol.TypeJoin, protocol.JoinData{RoomCode: "NOPE12", Nick: "x"})

	env := readType(t, c, protocol.TypeError)
	var e protocol.ErrorData
	_ = json.Unmarshal(env.Data, &e)
	if e.Code != "room_not_found" {
		t.Fatalf("want room_not_found, got %q", e.Code)
	}
}

func TestPingPong(t *testing.T) {
	srv := httptest.NewServer(New(room.NewRegistry(), nil))
	defer srv.Close()

	c := dial(t, wsURL(srv))
	defer c.Close(websocket.StatusNormalClosure, "")

	t0 := time.Now().UnixMilli()
	send(t, c, protocol.TypePing, protocol.PingData{T0: t0})

	env := readType(t, c, protocol.TypePong)
	var p protocol.PongData
	if err := json.Unmarshal(env.Data, &p); err != nil {
		t.Fatal(err)
	}
	if p.T0 != t0 {
		t.Fatalf("pong t0 mismatch: want %d got %d", t0, p.T0)
	}
	if p.T1 == 0 || p.T2 == 0 || p.T2 < p.T1 {
		t.Fatalf("bad server timestamps: %+v", p)
	}
}
