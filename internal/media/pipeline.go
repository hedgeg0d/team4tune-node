package media

import (
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

	mu     sync.Mutex
	tracks map[string]*protocol.Track
}

func New(cacheDir, baseURL string) (*Pipeline, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	return &Pipeline{
		cacheDir: cacheDir,
		baseURL:  strings.TrimRight(baseURL, "/"),
		tracks:   make(map[string]*protocol.Track),
	}, nil
}

func (p *Pipeline) CacheDir() string { return p.cacheDir }

func (p *Pipeline) Delete(id string) error {
	p.mu.Lock()
	delete(p.tracks, id)
	p.mu.Unlock()
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

	if title, ok := probeTitle(sourceURL); ok {
		p.update(id, func(t *protocol.Track) { t.Title = title }, onUpdate)
	}

	if _, err := os.Stat(out); err != nil {
		tmpl := filepath.Join(p.cacheDir, id+".%(ext)s")
		cmd := exec.Command("yt-dlp",
			"-x", "--audio-format", "opus",
			"--no-playlist",
			"--no-progress",
			"-o", tmpl,
			sourceURL,
		)
		if err := cmd.Run(); err != nil {
			p.update(id, func(t *protocol.Track) { t.Status = StatusError }, onUpdate)
			return
		}
		if !p.exists(id) {
			_ = p.Delete(id)
			return
		}
	}

	dur := probeDurationMs(out)
	fileURL := p.baseURL + "/media/" + id + ".opus"
	p.update(id, func(t *protocol.Track) {
		t.Status = StatusReady
		t.FileURL = fileURL
		t.DurationMs = dur
	}, onUpdate)
}

func trackID(sourceURL string) string {
	sum := sha256.Sum256([]byte(sourceURL))
	return hex.EncodeToString(sum[:])[:16]
}

func probeTitle(sourceURL string) (string, bool) {
	cmd := exec.Command("yt-dlp", "--no-playlist", "--skip-download", "--print", "%(title)s", sourceURL)
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	title := strings.TrimSpace(string(out))
	if title == "" || title == "NA" {
		return "", false
	}
	return title, true
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
