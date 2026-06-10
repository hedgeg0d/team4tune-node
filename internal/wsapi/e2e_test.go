package wsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/team4tune/node-server/internal/media"
	"github.com/team4tune/node-server/internal/protocol"
	"github.com/team4tune/node-server/internal/room"
)

func TestEnqueueEndToEnd(t *testing.T) {
	if os.Getenv("TEAM4TUNE_E2E") == "" {
		t.Skip("set TEAM4TUNE_E2E=1 to run network e2e (downloads via yt-dlp)")
	}

	url := os.Getenv("TEAM4TUNE_E2E_URL")
	if url == "" {
		url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
	}

	pipeline, err := media.New(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws", New(room.NewRegistry(), pipeline))
	mux.Handle("/media/", http.StripPrefix("/media/", http.FileServer(http.Dir(pipeline.CacheDir()))))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := dial(t, wsURL(srv))
	defer c.Close(websocket.StatusNormalClosure, "")

	send(t, c, protocol.TypeCreate, protocol.CreateData{Mode: protocol.ModeSignal, Nick: "host"})
	readType(t, c, protocol.TypeRoomState)
	send(t, c, protocol.TypeEnqueue, protocol.EnqueueData{SourceURL: url})

	var track protocol.Track
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		_, raw, err := c.Read(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var env protocol.Envelope
		if err := json.Unmarshal(raw, &env); err != nil || env.Type != protocol.TypeRoomState {
			continue
		}
		var st protocol.RoomStateData
		if err := json.Unmarshal(env.Data, &st); err != nil {
			t.Fatal(err)
		}
		if len(st.Queue) == 0 {
			continue
		}
		track = st.Queue[0]
		if track.Status == media.StatusReady {
			break
		}
		if track.Status == media.StatusError {
			t.Fatalf("track resolve errored")
		}
	}
	if track.Status != media.StatusReady {
		t.Fatalf("track not ready, status=%q", track.Status)
	}
	if track.DurationMs <= 0 {
		t.Fatalf("durationMs not set: %d", track.DurationMs)
	}

	req, _ := http.NewRequest("GET", srv.URL+"/media/"+track.ID+".opus", nil)
	req.Header.Set("Range", "bytes=0-99")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range request status = %d, want 206", resp.StatusCode)
	}
}
