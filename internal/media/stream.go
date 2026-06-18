package media

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	audioBitrate = "96k"
	segUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
	headBufferBytes    = 256 << 10
	liveHeadBytes      = 48 << 10
	tailPollInterval   = 40 * time.Millisecond
	tailChunkBytes     = 64 << 10
	segmentIdleTimeout = 30 * time.Second
	segmentReapTick    = 5 * time.Second
)

var (
	windowEngageBytes int64 = 8 << 20
	windowAheadBytes  int64 = 3 << 20
	windowBackBytes   int64 = 4 << 20
	windowHeaderKeep  int64 = 128 << 10
	windowPunchMin    int64 = 1 << 20
)

type mediaStream struct {
	mu        sync.Mutex
	written   int64
	observed  int64
	total     int64
	done      bool
	failed    bool
	aborted   bool
	abortCh   chan struct{}
	readers   int
	idleSince time.Time
}

func newMediaStream() *mediaStream {
	return &mediaStream{abortCh: make(chan struct{}), idleSince: time.Now()}
}

func (s *mediaStream) addReader() {
	s.mu.Lock()
	s.readers++
	s.mu.Unlock()
}

func (s *mediaStream) dropReader() {
	s.mu.Lock()
	s.readers--
	if s.readers <= 0 {
		s.readers = 0
		s.idleSince = time.Now()
	}
	s.mu.Unlock()
}

func (s *mediaStream) idleReapable(timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readers == 0 && time.Since(s.idleSince) > timeout
}

func (s *mediaStream) advance(n int64) {
	s.mu.Lock()
	s.written += n
	s.mu.Unlock()
}

func (s *mediaStream) observe(off int64) {
	s.mu.Lock()
	if off > s.observed {
		s.observed = off
	}
	s.mu.Unlock()
}

func (s *mediaStream) finish(total int64) {
	s.mu.Lock()
	s.total = total
	s.written = total
	s.done = true
	s.mu.Unlock()
}

func (s *mediaStream) fail() {
	s.mu.Lock()
	s.failed = true
	s.done = true
	s.mu.Unlock()
}

func (s *mediaStream) abort() {
	s.mu.Lock()
	if !s.aborted {
		s.aborted = true
		close(s.abortCh)
	}
	s.mu.Unlock()
}

func (s *mediaStream) snap() (written int64, done, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written, s.done, s.failed
}

func (s *mediaStream) frontier() (written, observed int64, aborted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written, s.observed, s.aborted
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
	if key == "" || strings.ContainsAny(key, "/\\") || strings.Contains(key, "..") {
		http.NotFound(w, r)
		return
	}

	if i := strings.Index(key, "__t"); i >= 0 {
		id := key[:i]
		sec, err := strconv.ParseInt(key[i+3:], 10, 64)
		if id == "" || err != nil || sec <= 0 {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		segID := id + "@" + strconv.FormatInt(sec, 10)
		seg := p.ensureSegment(id, segID, sec)
		if seg == nil {
			http.NotFound(w, r)
			return
		}
		p.serveStream(w, r, segID, seg)
		return
	}
	id := key

	p.mu.Lock()
	st := p.streams[id]
	p.mu.Unlock()
	if st != nil {
		p.serveStream(w, r, id, st)
		return
	}

	final := filepath.Join(p.cacheDir, id+".opus")
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
	http.ServeContent(w, r, id+".opus", fi.ModTime(), f)
}

func (p *Pipeline) serveStream(w http.ResponseWriter, r *http.Request, id string, st *mediaStream) {
	st.addReader()
	defer st.dropReader()
	part := filepath.Join(p.cacheDir, id+".part")
	f, err := os.Open(part)
	for err != nil {
		if fi, e := os.Stat(filepath.Join(p.cacheDir, id+".opus")); e == nil {
			if g, e2 := os.Open(filepath.Join(p.cacheDir, id+".opus")); e2 == nil {
				defer g.Close()
				http.ServeContent(w, r, id+".opus", fi.ModTime(), g)
				return
			}
		}
		if _, _, failed := st.snap(); failed || r.Context().Err() != nil {
			http.NotFound(w, r)
			return
		}
		select {
		case <-r.Context().Done():
			http.NotFound(w, r)
			return
		case <-time.After(tailPollInterval):
		}
		f, err = os.Open(part)
	}
	defer f.Close()

	w.Header().Set("Content-Type", "audio/ogg")

	start, _, _, ok := parseRange(r.Header.Get("Range"))
	if !ok || start != 0 {
		http.Error(w, "", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.WriteHeader(http.StatusOK)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
	p.waitAvail(r, st, liveHeadBytes)
	p.tailCopy(r, w, f, st)
}

func (p *Pipeline) tailCopy(r *http.Request, w http.ResponseWriter, f *os.File, st *mediaStream) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, tailChunkBytes)
	var off int64
	for {
		written, done, failed := st.snap()
		if failed {
			return
		}
		for off < written {
			n, err := f.ReadAt(buf, off)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return
				}
				off += int64(n)
				st.observe(off)
			}
			if err != nil && err != io.EOF {
				return
			}
			if n == 0 {
				break
			}
		}
		if flusher != nil {
			flusher.Flush()
		}
		if done && off >= written {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(tailPollInterval):
		}
	}
}

func (p *Pipeline) waitAvail(r *http.Request, st *mediaStream, want int64) (avail int64, done, failed bool) {
	for {
		w, d, f := st.snap()
		if w > want || d || f {
			return w, d, f
		}
		select {
		case <-r.Context().Done():
			return w, d, f
		case <-time.After(tailPollInterval):
		}
	}
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

func parseRange(h string) (start, end int64, hasRange, ok bool) {
	if h == "" {
		return 0, -1, false, true
	}
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, false
	}
	startStr := strings.TrimSpace(spec[:dash])
	endStr := strings.TrimSpace(spec[dash+1:])
	if startStr == "" {
		return 0, 0, false, false
	}
	s, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false, false
	}
	e := int64(-1)
	if endStr != "" {
		e, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || e < s {
			return 0, 0, false, false
		}
	}
	return s, e, true, true
}

func nextPunch(observed, lastPunch int64) (off, length int64, ok bool) {
	punchTo := observed - windowBackBytes
	if punchTo <= windowHeaderKeep {
		return 0, 0, false
	}
	from := lastPunch
	if from < windowHeaderKeep {
		from = windowHeaderKeep
	}
	if punchTo-from < windowPunchMin {
		return 0, 0, false
	}
	return from, punchTo - from, true
}
