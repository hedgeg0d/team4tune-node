package media

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

func TestResolveCacheHit(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available")
	}

	dir := t.TempDir()
	p, err := New(dir, "http://example.test")
	if err != nil {
		t.Fatal(err)
	}

	const source = "team4tune-test-source"
	id := trackID(source)
	out := filepath.Join(dir, id+".opus")

	gen := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi",
		"-i", "sine=frequency=440:duration=1", "-c:a", "libopus", out)
	if err := gen.Run(); err != nil {
		t.Fatalf("ffmpeg generate: %v", err)
	}

	ready := make(chan protocol.Track, 4)
	p.Resolve(source, func(tr protocol.Track) {
		if tr.Status == StatusReady || tr.Status == StatusError {
			ready <- tr
		}
	})

	select {
	case tr := <-ready:
		if tr.Status != StatusReady {
			t.Fatalf("status = %s, want ready", tr.Status)
		}
		if tr.DurationMs < 500 || tr.DurationMs > 2000 {
			t.Fatalf("durationMs = %d, want ~1000", tr.DurationMs)
		}
		if tr.FileURL != "http://example.test/media/"+id+".opus" {
			t.Fatalf("fileUrl = %s", tr.FileURL)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for ready")
	}
}
