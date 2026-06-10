package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/team4tune/node-server/internal/media"
)

func TestUploadOpus(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
	cacheDir := t.TempDir()
	src := filepath.Join(cacheDir, "sine.opus")
	gen := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-c:a", "libopus", "-b:a", "128k", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot produce opus: %v (%s)", err, out)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}

	p, err := media.New(cacheDir, "http://example.test")
	if err != nil {
		t.Fatal(err)
	}
	handler := Upload(p)

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	_ = mw.WriteField("title", "My Song.mp3")
	fw, err := mw.CreateFormFile("file", "My Song.opus")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write(data); err != nil {
		t.Fatal(err)
	}
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var resp uploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.SourceURL, "upload://") {
		t.Fatalf("sourceUrl = %q, want upload:// prefix", resp.SourceURL)
	}
	if resp.Title != "My Song.mp3" {
		t.Fatalf("title = %q", resp.Title)
	}
	if resp.DurationMs <= 0 {
		t.Fatalf("durationMs = %d, want > 0", resp.DurationMs)
	}

	id := strings.TrimPrefix(resp.SourceURL, "upload://")
	if _, err := os.Stat(filepath.Join(cacheDir, id+".opus")); err != nil {
		t.Fatalf("cached file missing: %v", err)
	}

	track := p.Resolve(resp.SourceURL, nil)
	if track.Status != media.StatusReady || track.FileURL == "" {
		t.Fatalf("resolve = %+v, want ready with fileURL", track)
	}
}

func TestUploadRejectsNonOpus(t *testing.T) {
	cacheDir := t.TempDir()
	p, err := media.New(cacheDir, "http://example.test")
	if err != nil {
		t.Fatal(err)
	}
	handler := Upload(p)

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	fw, _ := mw.CreateFormFile("file", "junk.opus")
	_, _ = io.WriteString(fw, "not really an opus file")
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415 (%s)", rec.Code, rec.Body.String())
	}
}

func TestUploadMethodNotAllowed(t *testing.T) {
	p, _ := media.New(t.TempDir(), "http://example.test")
	rec := httptest.NewRecorder()
	Upload(p)(rec, httptest.NewRequest(http.MethodGet, "/upload", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
