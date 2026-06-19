package media

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func newTestPipeline(t *testing.T) *Pipeline {
	t.Helper()
	p, err := New(t.TempDir(), "http://node")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func doGet(t *testing.T, p *Pipeline, id, rangeHdr string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/media/"+id+".opus", nil)
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	rec := httptest.NewRecorder()
	p.ServeMedia(rec, req)
	return rec.Result()
}

func TestServeMediaCompletedFileRange(t *testing.T) {
	p := newTestPipeline(t)
	id := "done1"
	want := []byte("0123456789abcdef")
	if err := os.WriteFile(filepath.Join(p.cacheDir, id+".opus"), want, 0o644); err != nil {
		t.Fatal(err)
	}

	resp := doGet(t, p, id, "bytes=4-7")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "4567" {
		t.Fatalf("body = %q, want %q", body, "4567")
	}
}

func TestServeMediaMidTrackSeekRange(t *testing.T) {
	p := newTestPipeline(t)
	id := "done2"
	want := []byte("0123456789abcdef")
	if err := os.WriteFile(filepath.Join(p.cacheDir, id+".opus"), want, 0o644); err != nil {
		t.Fatal(err)
	}

	resp := doGet(t, p, id, "bytes=10-")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "abcdef" {
		t.Fatalf("body = %q, want %q", body, "abcdef")
	}
}

func TestServeMediaMissingFile(t *testing.T) {
	p := newTestPipeline(t)
	resp := doGet(t, p, "nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
