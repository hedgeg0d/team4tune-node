package media

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/team4tune/node-server/internal/protocol"
)

func registerHLSTrack(p *Pipeline, tr protocol.Track) {
	p.mu.Lock()
	p.tracks[tr.ID] = &tr
	p.mu.Unlock()
}

func TestServeHLSPlaylist(t *testing.T) {
	p := newTestPipeline(t)
	registerHLSTrack(p, protocol.Track{
		ID:         "hls1",
		SourceURL:  "http://source.test/audio",
		Status:     StatusReady,
		FileURL:    "http://node/media/hls1/index.m3u8",
		DurationMs: 13500,
	})

	req := httptest.NewRequest(http.MethodGet, "/media/hls1/index.m3u8", nil)
	rec := httptest.NewRecorder()
	p.ServeMedia(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"#EXTM3U",
		"#EXT-X-PLAYLIST-TYPE:VOD",
		"#EXTINF:6.000,\nseg/0.ts",
		"#EXTINF:6.000,\nseg/1.ts",
		"#EXTINF:1.500,\nseg/2.ts",
		"#EXT-X-ENDLIST",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("playlist missing %q:\n%s", want, text)
		}
	}
}

func TestServeHLSSegmentGeneratesOnce(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "src.wav")
	gen := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=8", srcPath)
	if err := gen.Run(); err != nil {
		t.Skipf("could not generate source audio: %v", err)
	}

	fileSrv := httptest.NewServer(http.FileServer(http.Dir(srcDir)))
	defer fileSrv.Close()

	p := newTestPipeline(t)
	id := "hls2"
	registerHLSTrack(p, protocol.Track{
		ID:         id,
		SourceURL:  "http://source.test/audio",
		Status:     StatusReady,
		FileURL:    "http://node/media/hls2/index.m3u8",
		DurationMs: 8000,
	})
	p.mu.Lock()
	p.directURLs[id] = fileSrv.URL + "/src.wav"
	p.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/media/hls2/seg/0.ts", nil)
	rec := httptest.NewRecorder()
	p.ServeMedia(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("empty HLS segment")
	}
	if _, err := os.Stat(p.hlsSegmentPath(id, 0)); err != nil {
		t.Fatalf("cached segment missing: %v", err)
	}
}
