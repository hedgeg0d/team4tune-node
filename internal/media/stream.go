package media

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	audioBitrate = "96k"
	segUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
	copyBufBytes = 64 << 10
)

type mediaStream struct {
	mu      sync.Mutex
	aborted bool
	abortCh chan struct{}
}

func newMediaStream() *mediaStream { return &mediaStream{abortCh: make(chan struct{})} }

func (s *mediaStream) abort() {
	s.mu.Lock()
	if !s.aborted {
		s.aborted = true
		close(s.abortCh)
	}
	s.mu.Unlock()
}

func (s *mediaStream) isAborted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborted
}

func (p *Pipeline) ServeMedia(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/media/")
	if id, ok := hlsPlaylistID(name); ok {
		p.serveHLSPlaylist(w, r, id)
		return
	}
	if id, segment, ok := hlsSegmentID(name); ok {
		p.serveHLSSegment(w, r, id, segment)
		return
	}
	key := strings.TrimSuffix(name, ".opus")
	if !validMediaID(key) {
		http.NotFound(w, r)
		return
	}
	final := filepath.Join(p.cacheDir, key+".opus")
	fi, err := os.Stat(final)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(final)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	http.ServeContent(w, r, key+".opus", fi.ModTime(), f)
}

func hlsPlaylistID(name string) (string, bool) {
	id, ok := strings.CutSuffix(name, "/index.m3u8")
	if !ok || !validMediaID(id) {
		return "", false
	}
	return id, true
}

func hlsSegmentID(name string) (string, int64, bool) {
	id, rest, ok := strings.Cut(name, "/seg/")
	if !ok || !validMediaID(id) || !strings.HasSuffix(rest, ".ts") {
		return "", 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(rest, ".ts"), 10, 64)
	if err != nil || n < 0 {
		return "", 0, false
	}
	return id, n, true
}

func validMediaID(id string) bool {
	return id != "" && !strings.ContainsAny(id, "/\\") && !strings.Contains(id, "..")
}
