package media

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

const uploadPrefix = "upload://"

var ErrNotOpus = errors.New("not an opus file")

const (
	StatusResolving = protocol.StatusResolving
	StatusReady     = protocol.StatusReady
	StatusError     = protocol.StatusError
)

type Pipeline struct {
	cacheDir string
	baseURL  string

	mu         sync.Mutex
	tracks     map[string]*protocol.Track
	streams    map[string]*mediaStream
	directURLs map[string]string
	hlsJobs    map[string]*hlsJob
}

func New(cacheDir, baseURL string) (*Pipeline, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	return &Pipeline{
		cacheDir:   cacheDir,
		baseURL:    strings.TrimRight(baseURL, "/"),
		tracks:     make(map[string]*protocol.Track),
		streams:    make(map[string]*mediaStream),
		directURLs: make(map[string]string),
		hlsJobs:    make(map[string]*hlsJob),
	}, nil
}

func (p *Pipeline) CacheDir() string { return p.cacheDir }

func (p *Pipeline) Delete(id string) error {
	p.mu.Lock()
	delete(p.tracks, id)
	delete(p.directURLs, id)
	var aborts []*mediaStream
	for key, st := range p.streams {
		if key == id || strings.HasPrefix(key, id+"@") {
			aborts = append(aborts, st)
		}
	}
	p.mu.Unlock()
	for _, st := range aborts {
		st.abort()
	}
	paths, err := filepath.Glob(filepath.Join(p.cacheDir, id+"*"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (p *Pipeline) exists(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.tracks[id]
	return ok
}

func (p *Pipeline) idFor(sourceURL string) string {
	if strings.HasPrefix(sourceURL, uploadPrefix) {
		return strings.TrimPrefix(sourceURL, uploadPrefix)
	}
	return trackID(sourceURL)
}

func (p *Pipeline) RegisterOpus(r io.Reader, title string) (protocol.Track, error) {
	tmp, err := os.CreateTemp(p.cacheDir, "upload-*.tmp")
	if err != nil {
		return protocol.Track{}, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	h := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(r, h)); err != nil {
		tmp.Close()
		return protocol.Track{}, err
	}
	if err := tmp.Close(); err != nil {
		return protocol.Track{}, err
	}

	id := hex.EncodeToString(h.Sum(nil))[:16]
	out := filepath.Join(p.cacheDir, id+".opus")
	if _, err := os.Stat(out); err != nil {
		if !probeIsOpus(tmpPath) {
			return protocol.Track{}, ErrNotOpus
		}
		if err := os.Rename(tmpPath, out); err != nil {
			return protocol.Track{}, err
		}
	}

	if title == "" {
		title = "Local file"
	}
	track := protocol.Track{
		ID:         id,
		SourceURL:  uploadPrefix + id,
		Status:     StatusReady,
		Title:      title,
		FileURL:    p.baseURL + "/media/" + id + ".opus",
		DurationMs: probeDurationMs(out),
	}
	p.mu.Lock()
	t := track
	p.tracks[id] = &t
	p.mu.Unlock()
	return track, nil
}

func (p *Pipeline) Resolve(sourceURL string, onUpdate func(protocol.Track)) protocol.Track {
	id := p.idFor(sourceURL)

	p.mu.Lock()
	if t, ok := p.tracks[id]; ok {
		snap := *t
		p.mu.Unlock()
		return snap
	}
	t := &protocol.Track{
		ID:        id,
		SourceURL: sourceURL,
		Status:    StatusResolving,
	}
	p.tracks[id] = t
	snap := *t
	p.mu.Unlock()

	go p.run(id, sourceURL, onUpdate)
	return snap
}

func (p *Pipeline) Get(id string) (protocol.Track, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.tracks[id]
	if !ok {
		return protocol.Track{}, false
	}
	return *t, true
}

func (p *Pipeline) update(id string, fn func(t *protocol.Track), onUpdate func(protocol.Track)) {
	p.mu.Lock()
	t, ok := p.tracks[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	fn(t)
	snap := *t
	p.mu.Unlock()
	if onUpdate != nil {
		onUpdate(snap)
	}
}

func (p *Pipeline) run(id, sourceURL string, onUpdate func(protocol.Track)) {
	out := filepath.Join(p.cacheDir, id+".opus")
	fileURL := p.baseURL + "/media/" + id + ".opus"

	title, durMs := probeMeta(sourceURL)
	if title != "" {
		p.update(id, func(t *protocol.Track) { t.Title = title }, onUpdate)
	}

	if _, err := os.Stat(out); err == nil {
		if durMs == 0 {
			durMs = probeDurationMs(out)
		}
		p.markReady(id, fileURL, durMs, onUpdate)
		return
	}

	if !strings.HasPrefix(sourceURL, uploadPrefix) {
		go p.directURLFor(id, sourceURL)
		if durMs >= hlsDurationThresholdMs {
			p.markReady(id, p.baseURL+"/media/"+id+"/index.m3u8", durMs, onUpdate)
			return
		}
	}

	st := newMediaStream()
	p.mu.Lock()
	p.streams[id] = st
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.streams, id)
		p.mu.Unlock()
	}()

	part := filepath.Join(p.cacheDir, id+".part")
	_ = os.Remove(part)
	pf, err := os.Create(part)
	if err != nil {
		st.fail()
		p.setError(id, onUpdate)
		return
	}

	ytdlp := exec.Command("yt-dlp",
		"-f", "bestaudio/best",
		"--no-playlist", "--no-progress",
		"-o", "-", sourceURL,
	)
	ff := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-i", "pipe:0",
		"-vn", "-c:a", "libopus", "-b:a", audioBitrate,
		"-f", "ogg", "pipe:1",
	)
	ytOut, err := ytdlp.StdoutPipe()
	if err != nil {
		pf.Close()
		st.fail()
		p.setError(id, onUpdate)
		return
	}
	ff.Stdin = ytOut
	ffOut, err := ff.StdoutPipe()
	if err != nil {
		pf.Close()
		st.fail()
		p.setError(id, onUpdate)
		return
	}
	if err := ytdlp.Start(); err != nil || ff.Start() != nil {
		pf.Close()
		st.fail()
		_ = os.Remove(part)
		p.setError(id, onUpdate)
		return
	}

	go func() {
		<-st.abortCh
		if ytdlp.Process != nil {
			_ = ytdlp.Process.Kill()
		}
		if ff.Process != nil {
			_ = ff.Process.Kill()
		}
	}()

	var headReady func()
	if durMs > 0 {
		headReady = func() { p.markReady(id, fileURL, durMs, onUpdate) }
	}
	total, copyErr := p.pump(st, ffOut, pf, headReady)
	pf.Close()
	ffErr := ff.Wait()
	_ = ytdlp.Wait()

	if _, _, aborted := st.frontier(); aborted {
		st.fail()
		_ = os.Remove(part)
		return
	}
	if copyErr != nil || ffErr != nil || total == 0 {
		st.fail()
		_ = os.Remove(part)
		p.setError(id, onUpdate)
		return
	}
	if !p.exists(id) {
		st.fail()
		_ = os.Remove(part)
		_ = p.Delete(id)
		return
	}
	if err := os.Rename(part, out); err != nil {
		st.fail()
		p.setError(id, onUpdate)
		return
	}
	st.finish(total)
	if durMs == 0 {
		durMs = probeDurationMs(out)
	}
	p.markReady(id, fileURL, durMs, onUpdate)
}

func (p *Pipeline) pump(st *mediaStream, out io.Reader, pf *os.File, headReady func()) (total int64, err error) {
	var lastPunch int64
	windowed := false
	buf := make([]byte, tailChunkBytes)
	for {
		if windowed {
			for {
				_, observed, aborted := st.frontier()
				if aborted || total-observed < windowAheadBytes {
					break
				}
				select {
				case <-st.abortCh:
				case <-time.After(tailPollInterval):
				}
			}
		}
		if _, _, aborted := st.frontier(); aborted {
			return total, nil
		}
		n, er := out.Read(buf)
		if n > 0 {
			if _, we := pf.Write(buf[:n]); we != nil {
				return total, we
			}
			total += int64(n)
			st.advance(int64(n))
			if !windowed && total >= windowEngageBytes {
				windowed = true
			}
			if windowed {
				_, observed, _ := st.frontier()
				if off, length, ok := nextPunch(observed, lastPunch); ok {
					_ = punchHole(pf, off, length)
					lastPunch = off + length
				}
			}
			if headReady != nil && total >= headBufferBytes {
				headReady()
				headReady = nil
			}
		}
		if er != nil {
			if er != io.EOF {
				return total, er
			}
			return total, nil
		}
	}
}

func (p *Pipeline) directURLFor(id, sourceURL string) string {
	p.mu.Lock()
	u := p.directURLs[id]
	p.mu.Unlock()
	if u != "" {
		return u
	}
	out, err := exec.Command("yt-dlp", "-f", "bestaudio/best", "--no-playlist",
		"--extractor-args", "youtube:player_client=web", "-g", sourceURL).Output()
	if err != nil {
		return ""
	}
	u = strings.TrimSpace(strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0])
	if u == "" {
		return ""
	}
	p.mu.Lock()
	p.directURLs[id] = u
	p.mu.Unlock()
	return u
}

func (p *Pipeline) ensureSegment(id, segID string, startSec int64) *mediaStream {
	p.mu.Lock()
	if st := p.streams[segID]; st != nil {
		p.mu.Unlock()
		return st
	}
	t, ok := p.tracks[id]
	if !ok || t.SourceURL == "" || strings.HasPrefix(t.SourceURL, uploadPrefix) {
		p.mu.Unlock()
		return nil
	}
	sourceURL := t.SourceURL
	st := newMediaStream()
	p.streams[segID] = st
	p.mu.Unlock()
	go p.runSegment(id, segID, sourceURL, startSec, st)
	return st
}

func (p *Pipeline) runSegment(id, segID, sourceURL string, startSec int64, st *mediaStream) {
	part := filepath.Join(p.cacheDir, segID+".part")
	_ = os.Remove(part)
	pf, err := os.Create(part)
	if err != nil {
		st.fail()
		p.mu.Lock()
		delete(p.streams, segID)
		p.mu.Unlock()
		return
	}

	direct := p.directURLFor(id, sourceURL)
	if direct == "" {
		pf.Close()
		_ = os.Remove(part)
		st.fail()
		p.mu.Lock()
		delete(p.streams, segID)
		p.mu.Unlock()
		return
	}

	ff := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-user_agent", segUserAgent,
		"-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
		"-ss", strconv.FormatInt(startSec, 10),
		"-i", direct,
		"-vn", "-c:a", "libopus", "-b:a", audioBitrate,
		"-f", "ogg", "pipe:1",
	)
	ffOut, err := ff.StdoutPipe()
	if err != nil || ff.Start() != nil {
		pf.Close()
		st.fail()
		_ = os.Remove(part)
		p.mu.Lock()
		delete(p.streams, segID)
		p.mu.Unlock()
		return
	}
	go func() {
		<-st.abortCh
		if ff.Process != nil {
			_ = ff.Process.Kill()
		}
	}()
	go p.reapWhenIdle(segID, st)

	total, copyErr := p.pump(st, ffOut, pf, nil)
	pf.Close()
	ffErr := ff.Wait()

	if _, _, aborted := st.frontier(); aborted {
		st.fail()
		_ = os.Remove(part)
		p.mu.Lock()
		delete(p.streams, segID)
		p.mu.Unlock()
		return
	}
	if copyErr != nil || ffErr != nil || total == 0 {
		st.fail()
		_ = os.Remove(part)
		p.mu.Lock()
		delete(p.streams, segID)
		p.directURLs[id] = ""
		p.mu.Unlock()
		return
	}
	st.finish(total)
}

func (p *Pipeline) reapWhenIdle(segID string, st *mediaStream) {
	ticker := time.NewTicker(segmentReapTick)
	defer ticker.Stop()
	for {
		select {
		case <-st.abortCh:
			return
		case <-ticker.C:
			if st.idleReapable(segmentIdleTimeout) {
				st.abort()
				p.mu.Lock()
				if p.streams[segID] == st {
					delete(p.streams, segID)
				}
				p.mu.Unlock()
				_ = os.Remove(filepath.Join(p.cacheDir, segID+".part"))
				return
			}
		}
	}
}

func (p *Pipeline) markReady(id, fileURL string, durMs int64, onUpdate func(protocol.Track)) {
	p.update(id, func(t *protocol.Track) {
		t.Status = StatusReady
		t.FileURL = fileURL
		t.DurationMs = durMs
	}, onUpdate)
}

func (p *Pipeline) setError(id string, onUpdate func(protocol.Track)) {
	p.update(id, func(t *protocol.Track) { t.Status = StatusError }, onUpdate)
}

func trackID(sourceURL string) string {
	sum := sha256.Sum256([]byte(sourceURL))
	return hex.EncodeToString(sum[:])[:16]
}

func probeMeta(sourceURL string) (title string, durationMs int64) {
	cmd := exec.Command("yt-dlp", "--no-playlist", "--skip-download",
		"--print", "%(title)s", "--print", "%(duration)s", sourceURL)
	out, err := cmd.Output()
	if err != nil {
		return "", 0
	}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	if sc.Scan() {
		t := strings.TrimSpace(sc.Text())
		if t != "" && t != "NA" {
			title = t
		}
	}
	if sc.Scan() {
		if sec, err := strconv.ParseFloat(strings.TrimSpace(sc.Text()), 64); err == nil {
			durationMs = int64(sec * 1000)
		}
	}
	return title, durationMs
}

func probeIsOpus(path string) bool {
	cmd := exec.Command("ffprobe", "-v", "error",
		"-select_streams", "a:0",
		"-show_entries", "stream=codec_name",
		"-of", "default=nw=1:nokey=1", path)
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "opus"
}

func probeDurationMs(path string) int64 {
	cmd := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=nw=1:nokey=1", path)
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	sec, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return int64(sec * 1000)
}
