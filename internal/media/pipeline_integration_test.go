package media

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

func requireTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"yt-dlp", "ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not installed", tool)
		}
	}
}

func TestResolveStreamsLocalSource(t *testing.T) {
	requireTools(t)

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "src.mp3")
	gen := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=6",
		"-c:a", "libmp3lame", srcPath)
	if err := gen.Run(); err != nil {
		t.Skipf("could not generate source audio: %v", err)
	}

	fileSrv := httptest.NewServer(http.FileServer(http.Dir(srcDir)))
	defer fileSrv.Close()

	p := newTestPipeline(t)
	media := httptest.NewServer(http.HandlerFunc(p.ServeMedia))
	defer media.Close()

	var mu sync.Mutex
	var latest protocol.Track
	p.Resolve(fileSrv.URL+"/src.mp3", func(tr protocol.Track) {
		mu.Lock()
		latest = tr
		mu.Unlock()
	})

	deadline := time.Now().Add(60 * time.Second)
	var ready protocol.Track
	for time.Now().Before(deadline) {
		mu.Lock()
		cur := latest
		mu.Unlock()
		if cur.Status == StatusError {
			t.Fatal("resolve errored")
		}
		if cur.Status == StatusReady {
			ready = cur
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ready.Status != StatusReady {
		t.Fatal("track never became ready")
	}
	if ready.DurationMs < 4000 || ready.DurationMs > 9000 {
		t.Fatalf("durationMs = %d, want ~6000", ready.DurationMs)
	}

	req, _ := http.NewRequest(http.MethodGet, media.URL+"/media/"+ready.ID+".opus", nil)
	req.Header.Set("Range", "bytes=0-99")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status = %d, want 206", resp.StatusCode)
	}

	final := filepath.Join(p.cacheDir, ready.ID+".opus")
	for time.Now().Before(deadline) {
		if _, err := os.Stat(final); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	fi, err := os.Stat(final)
	if err != nil {
		t.Fatalf("final opus never produced: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("final opus is empty")
	}
}

func TestSeekSegment(t *testing.T) {
	requireTools(t)

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "src.mp3")
	gen := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=12",
		"-c:a", "libmp3lame", srcPath)
	if err := gen.Run(); err != nil {
		t.Skipf("could not generate source audio: %v", err)
	}

	fileSrv := httptest.NewServer(http.FileServer(http.Dir(srcDir)))
	defer fileSrv.Close()

	p := newTestPipeline(t)
	media := httptest.NewServer(http.HandlerFunc(p.ServeMedia))
	defer media.Close()

	initial := p.Resolve(fileSrv.URL+"/src.mp3", nil)
	id := initial.ID

	req, _ := http.NewRequest(http.MethodGet, media.URL+"/media/"+id+"__t4.opus", nil)
	req.Header.Set("Range", "bytes=0-")
	var resp *http.Response
	var err error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
		req, _ = http.NewRequest(http.MethodGet, media.URL+"/media/"+id+"__t4.opus", nil)
		req.Header.Set("Range", "bytes=0-")
	}
	if err != nil {
		t.Fatalf("segment request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("segment status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) < 4 || string(body[:4]) != "OggS" {
		t.Fatalf("segment did not start with Ogg magic, got %d bytes", len(body))
	}

	out, derr := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration", "-of", "default=nw=1:nokey=1",
		filepath.Join(p.cacheDir, id+"@4.part")).Output()
	if derr != nil {
		t.Fatalf("segment not probeable: %v", derr)
	}
	if d, perr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); perr != nil || d < 6 || d > 9 {
		t.Fatalf("segment duration = %q, want ~8s", out)
	}
}
