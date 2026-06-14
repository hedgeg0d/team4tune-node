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

	mu      sync.Mutex
	tracks  map[string]*protocol.Track
	streams map[string]*mediaStream
}

func New(cacheDir, baseURL string) (*Pipeline, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	return &Pipeline{
		cacheDir: cacheDir,
		baseURL:  strings.TrimRight(baseURL, "/"),
		tracks:   make(map[string]*protocol.Track),
		streams:  make(map[string]*mediaStream),
	}, nil
}

func (p *Pipeline) CacheDir() string { return p.cacheDir }

func (p *Pipeline) Delete(id string) error {
	p.mu.Lock()
	delete(p.tracks, id)
	st := p.streams[id]
	p.mu.Unlock()
	if st != nil {
		st.abort()
	}
	paths, err := filepath.Glob(filepath.Join(p.cacheDir, id+".*"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
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

	var total, lastPunch int64
	var copyErr error
	readyMarked := false
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
			break
		}
		n, er := ffOut.Read(buf)
		if n > 0 {
			if _, we := pf.Write(buf[:n]); we != nil {
				copyErr = we
				break
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
			if !readyMarked && durMs > 0 && total >= headBufferBytes {
				readyMarked = true
				p.markReady(id, fileURL, durMs, onUpdate)
			}
		}
		if er != nil {
			if er != io.EOF {
				copyErr = er
			}
			break
		}
	}
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
