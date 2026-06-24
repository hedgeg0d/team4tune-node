package media

import (
	"bufio"
	"context"
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

const ytdlpTimeout = 25 * time.Second

var (
	ErrNotOpus   = errors.New("not an opus file")
	ErrTranscode = errors.New("transcode failed")
)

const (
	StatusResolving = protocol.StatusResolving
	StatusReady     = protocol.StatusReady
	StatusError     = protocol.StatusError
)

type Pipeline struct {
	cacheDir string
	baseURL  string

	mu          sync.Mutex
	tracks      map[string]*protocol.Track
	streams     map[string]*mediaStream
	directURLs  map[string]string
	hlsJobs     map[string]*hlsJob
	segSem      chan struct{}
	notify      map[string]func(protocol.Track)
	fullJobs    map[string]chan struct{}
	fullDesired map[string]bool
	ctxs        map[string]context.Context
	cancels     map[string]context.CancelFunc
}

func New(cacheDir, baseURL string) (*Pipeline, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	return &Pipeline{
		cacheDir:    cacheDir,
		baseURL:     strings.TrimRight(baseURL, "/"),
		tracks:      make(map[string]*protocol.Track),
		streams:     make(map[string]*mediaStream),
		directURLs:  make(map[string]string),
		hlsJobs:     make(map[string]*hlsJob),
		segSem:      make(chan struct{}, hlsSegConcurrency),
		notify:      make(map[string]func(protocol.Track)),
		fullJobs:    make(map[string]chan struct{}),
		fullDesired: make(map[string]bool),
		ctxs:        make(map[string]context.Context),
		cancels:     make(map[string]context.CancelFunc),
	}, nil
}

func (p *Pipeline) ctxFor(id string) context.Context {
	p.mu.Lock()
	ctx := p.ctxs[id]
	p.mu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (p *Pipeline) CacheDir() string { return p.cacheDir }

func (p *Pipeline) Delete(id string) error {
	p.mu.Lock()
	delete(p.tracks, id)
	delete(p.directURLs, id)
	delete(p.notify, id)
	delete(p.fullDesired, id)
	if cancel := p.cancels[id]; cancel != nil {
		cancel()
	}
	delete(p.cancels, id)
	delete(p.ctxs, id)
	var aborts []*mediaStream
	if st := p.streams[id]; st != nil {
		aborts = append(aborts, st)
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
	if _, ok := p.cancels[id]; !ok {
		ctx, cancel := context.WithCancel(context.Background())
		p.ctxs[id] = ctx
		p.cancels[id] = cancel
	}
	p.mu.Unlock()
	return track, nil
}

func (p *Pipeline) Resolve(sourceURL string, onUpdate func(protocol.Track)) protocol.Track {
	id := p.idFor(sourceURL)

	p.mu.Lock()
	if onUpdate != nil {
		p.notify[id] = onUpdate
	}
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
	ctx, cancel := context.WithCancel(context.Background())
	p.ctxs[id] = ctx
	p.cancels[id] = cancel
	snap := *t
	p.mu.Unlock()

	go p.run(id, sourceURL, onUpdate)
	return snap
}

func (p *Pipeline) StreamInput(t protocol.Track) (string, bool) {
	opus := filepath.Join(p.cacheDir, t.ID+".opus")
	if _, err := os.Stat(opus); err == nil {
		return opus, true
	}
	if strings.HasPrefix(t.SourceURL, uploadPrefix) {
		return "", false
	}
	direct := p.directURLFor(t.ID, t.SourceURL)
	if direct == "" {
		return "", false
	}
	return direct, true
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

	if !strings.HasPrefix(sourceURL, uploadPrefix) && durMs >= hlsDurationThresholdMs {
		go p.directURLFor(id, sourceURL)
		p.markReady(id, p.baseURL+"/media/"+id+"/index.m3u8", durMs, onUpdate)
		return
	}

	if err := p.transcodeComplete(id, sourceURL, out); err != nil {
		p.setError(id, onUpdate)
		return
	}
	if durMs == 0 {
		durMs = probeDurationMs(out)
	}
	p.markReady(id, fileURL, durMs, onUpdate)
}

func (p *Pipeline) transcodeComplete(id, sourceURL, out string) error {
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
		return err
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
		return err
	}
	ff.Stdin = ytOut
	ffOut, err := ff.StdoutPipe()
	if err != nil {
		pf.Close()
		return err
	}
	if err := ytdlp.Start(); err != nil {
		pf.Close()
		_ = os.Remove(part)
		return err
	}
	if err := ff.Start(); err != nil {
		pf.Close()
		_ = ytdlp.Process.Kill()
		_ = os.Remove(part)
		return err
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

	buf := make([]byte, copyBufBytes)
	total, copyErr := io.CopyBuffer(pf, ffOut, buf)
	pf.Close()
	ffErr := ff.Wait()
	_ = ytdlp.Wait()

	if st.isAborted() || copyErr != nil || ffErr != nil || total == 0 {
		_ = os.Remove(part)
		return ErrTranscode
	}
	if err := os.Rename(part, out); err != nil {
		return err
	}
	return nil
}

func (p *Pipeline) directURLFor(id, sourceURL string) string {
	p.mu.Lock()
	u := p.directURLs[id]
	p.mu.Unlock()
	if u != "" {
		return u
	}
	ctx, cancel := context.WithTimeout(p.ctxFor(id), ytdlpTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "yt-dlp", "-f", "bestaudio[ext=m4a]/bestaudio/best", "--no-playlist",
		"--extractor-args", "youtube:player_client=android_vr", "-g", sourceURL).Output()
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

func (p *Pipeline) clearDirectURL(id string) {
	p.mu.Lock()
	delete(p.directURLs, id)
	p.mu.Unlock()
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
	ctx, cancel := context.WithTimeout(context.Background(), ytdlpTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "yt-dlp", "--no-playlist", "--skip-download",
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
