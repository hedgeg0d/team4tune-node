package media

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestPipeline(t *testing.T) *Pipeline {
	t.Helper()
	p, err := New(t.TempDir(), "http://node")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func registerPart(t *testing.T, p *Pipeline, id string, data []byte) *mediaStream {
	t.Helper()
	part := filepath.Join(p.cacheDir, id+".part")
	if err := os.WriteFile(part, data, 0o644); err != nil {
		t.Fatal(err)
	}
	st := newMediaStream()
	st.advance(int64(len(data)))
	p.mu.Lock()
	p.streams[id] = st
	p.mu.Unlock()
	return st
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

func TestServeMediaCompletedFile(t *testing.T) {
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

func TestServeMediaInProgressRange(t *testing.T) {
	p := newTestPipeline(t)
	id := "live1"
	registerPart(t, p, id, []byte("0123456789"))

	resp := doGet(t, p, id, "bytes=2-5")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 2-5/*" {
		t.Fatalf("Content-Range = %q, want %q", cr, "bytes 2-5/*")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "2345" {
		t.Fatalf("body = %q, want %q", body, "2345")
	}
}

func TestServeMediaOpenRangeServesAvailable(t *testing.T) {
	p := newTestPipeline(t)
	id := "live2"
	registerPart(t, p, id, []byte("abcdef"))

	resp := doGet(t, p, id, "bytes=2-")
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "cdef" {
		t.Fatalf("body = %q, want %q", body, "cdef")
	}
	if cl := resp.Header.Get("Content-Length"); cl != "4" {
		t.Fatalf("Content-Length = %q, want 4", cl)
	}
}

func TestServeMediaWaitsForBytesThenFinishes(t *testing.T) {
	p := newTestPipeline(t)
	id := "live3"
	part := filepath.Join(p.cacheDir, id+".part")
	if err := os.WriteFile(part, []byte("AAAA"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := newMediaStream()
	st.advance(4)
	p.mu.Lock()
	p.streams[id] = st
	p.mu.Unlock()

	go func() {
		time.Sleep(60 * time.Millisecond)
		f, _ := os.OpenFile(part, os.O_APPEND|os.O_WRONLY, 0o644)
		f.Write([]byte("BBBB"))
		f.Close()
		st.advance(4)
		st.finish(8)
	}()

	resp := doGet(t, p, id, "bytes=6-")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 6-7/8" {
		t.Fatalf("Content-Range = %q, want %q", cr, "bytes 6-7/8")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "BB" {
		t.Fatalf("body = %q, want %q", body, "BB")
	}
}

func TestServeMediaNoRangeTailsUntilDone(t *testing.T) {
	p := newTestPipeline(t)
	id := "live4"
	part := filepath.Join(p.cacheDir, id+".part")
	if err := os.WriteFile(part, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := newMediaStream()
	st.advance(5)
	p.mu.Lock()
	p.streams[id] = st
	p.mu.Unlock()

	go func() {
		time.Sleep(60 * time.Millisecond)
		f, _ := os.OpenFile(part, os.O_APPEND|os.O_WRONLY, 0o644)
		f.Write([]byte(" world"))
		f.Close()
		st.advance(6)
		st.finish(11)
	}()

	resp := doGet(t, p, id, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello world" {
		t.Fatalf("body = %q, want %q", body, "hello world")
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		in              string
		start, end      int64
		hasRange, valid bool
	}{
		{"", 0, -1, false, true},
		{"bytes=0-99", 0, 99, true, true},
		{"bytes=10-", 10, -1, true, true},
		{"bytes=5-2", 0, 0, false, false},
		{"bytes=-100", 0, 0, false, false},
		{"items=0-1", 0, 0, false, false},
		{"bytes=0-1,5-6", 0, 0, false, false},
	}
	for _, c := range cases {
		s, e, hr, ok := parseRange(c.in)
		if ok != c.valid || hr != c.hasRange || (ok && (s != c.start || e != c.end)) {
			t.Errorf("parseRange(%q) = (%d,%d,%v,%v), want (%d,%d,%v,%v)",
				c.in, s, e, hr, ok, c.start, c.end, c.hasRange, c.valid)
		}
	}
}
