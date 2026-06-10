package wsapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/coder/websocket"
	"github.com/team4tune/node-server/internal/media"
	"github.com/team4tune/node-server/internal/protocol"
	"github.com/team4tune/node-server/internal/room"
)

func TestSignalFlowOverWire(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}

	const source = "local-signal-test"
	sum := sha256.Sum256([]byte(source))
	id := hex.EncodeToString(sum[:])[:16]

	dir := t.TempDir()
	out := filepath.Join(dir, id+".opus")
	gen := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi",
		"-i", "sine=frequency=440:duration=1", "-c:a", "libopus", out)
	if err := gen.Run(); err != nil {
		t.Fatalf("seed opus: %v", err)
	}

	pipeline, err := media.New(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws", New(room.NewRegistry(), pipeline))
	mux.Handle("/media/", http.StripPrefix("/media/", http.FileServer(http.Dir(pipeline.CacheDir()))))
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

	send(t, host, protocol.TypeEnqueue, protocol.EnqueueData{SourceURL: source})

	for _, c := range []*websocket.Conn{host, guest} {
		var p protocol.PrepareData
		_ = json.Unmarshal(readType(t, c, protocol.TypePrepare).Data, &p)
		if p.TrackID != id {
			t.Fatalf("prepare trackID = %q, want %q", p.TrackID, id)
		}
	}

	send(t, host, protocol.TypeReady, protocol.ReadyData{TrackID: id})
	send(t, guest, protocol.TypeReady, protocol.ReadyData{TrackID: id})

	var hostT0, guestT0 int64
	for i, c := range []*websocket.Conn{host, guest} {
		var np protocol.NowPlayingData
		_ = json.Unmarshal(readType(t, c, protocol.TypeNowPlaying).Data, &np)
		if np.TrackID != id {
			t.Fatalf("now_playing trackID = %q, want %q", np.TrackID, id)
		}
		if i == 0 {
			hostT0 = np.T0
		} else {
			guestT0 = np.T0
		}
	}
	if hostT0 != guestT0 {
		t.Fatalf("t0 differs between clients: host=%d guest=%d", hostT0, guestT0)
	}
}
