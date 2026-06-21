package media

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	m4aBytesPerSec  int64 = 16500
	opusBytesPerSec int64 = 12500
)

type ReconcileTrack struct {
	ID         string
	SourceURL  string
	DurationMs int64
	Playing    bool
}

func estimateM4ABytes(durMs int64) int64  { return durMs / 1000 * m4aBytesPerSec }
func estimateOpusBytes(durMs int64) int64 { return durMs / 1000 * opusBytesPerSec }

func planFull(tracks []ReconcileTrack, budgetMB int) map[string]bool {
	budget := int64(budgetMB) << 20
	var used int64
	plan := make(map[string]bool, len(tracks))
	for _, t := range tracks {
		if strings.HasPrefix(t.SourceURL, uploadPrefix) || t.DurationMs < hlsDurationThresholdMs {
			used += estimateOpusBytes(t.DurationMs)
			continue
		}
		size := estimateM4ABytes(t.DurationMs)
		if used+size <= budget {
			used += size
			plan[t.ID] = true
		} else {
			plan[t.ID] = false
		}
	}
	return plan
}

func (p *Pipeline) Reconcile(tracks []ReconcileTrack, budgetMB int) {
	plan := planFull(tracks, budgetMB)
	for _, t := range tracks {
		full, ok := plan[t.ID]
		if !ok {
			continue
		}
		if full {
			p.ensureFull(t.ID, t.SourceURL)
		} else if !t.Playing {
			p.evictFull(t.ID)
		}
	}
}

func (p *Pipeline) fullPath(id string) string {
	return filepath.Join(p.cacheDir, id+".m4a")
}

func (p *Pipeline) ensureFull(id, sourceURL string) {
	p.mu.Lock()
	p.fullDesired[id] = true
	p.mu.Unlock()
	out := p.fullPath(id)
	if fi, err := os.Stat(out); err == nil && fi.Size() > 0 {
		p.setFullReady(id)
		return
	}
	p.mu.Lock()
	if _, ok := p.fullJobs[id]; ok {
		p.mu.Unlock()
		return
	}
	done := make(chan struct{})
	p.fullJobs[id] = done
	p.mu.Unlock()

	go func() {
		err := p.downloadFullM4A(id, sourceURL, out)
		p.mu.Lock()
		delete(p.fullJobs, id)
		p.mu.Unlock()
		close(done)
		if err == nil {
			p.setFullReady(id)
		}
	}()
}

func (p *Pipeline) downloadFullM4A(id, sourceURL, out string) error {
	prefix := filepath.Join(p.cacheDir, id+".fulltmp.")
	for _, m := range globIgnore(prefix + "*") {
		_ = os.Remove(m)
	}
	cmd := exec.Command("yt-dlp",
		"-f", "140/bestaudio[ext=m4a]", "--no-playlist",
		"--extractor-args", "youtube:player_client=android_vr",
		"--http-chunk-size", "1M", "-N", "4",
		"-o", prefix+"%(ext)s", sourceURL)
	if err := cmd.Run(); err != nil {
		for _, m := range globIgnore(prefix + "*") {
			_ = os.Remove(m)
		}
		return err
	}
	matches := globIgnore(prefix + "*")
	if len(matches) == 0 {
		return os.ErrNotExist
	}
	return os.Rename(matches[0], out)
}

func globIgnore(pattern string) []string {
	m, _ := filepath.Glob(pattern)
	return m
}

func (p *Pipeline) setFullReady(id string) {
	url := p.baseURL + "/media/" + id + ".m4a"
	p.mu.Lock()
	t, ok := p.tracks[id]
	if !ok || !p.fullDesired[id] {
		p.mu.Unlock()
		_ = os.Remove(p.fullPath(id))
		return
	}
	if t.FileURL == url {
		p.mu.Unlock()
		return
	}
	t.FileURL = url
	t.Status = StatusReady
	p.mu.Unlock()
	p.emitUpdate(id)
}

func (p *Pipeline) evictFull(id string) {
	full := p.baseURL + "/media/" + id + ".m4a"
	p.mu.Lock()
	p.fullDesired[id] = false
	t, ok := p.tracks[id]
	revert := ok && t.FileURL == full
	if revert {
		t.FileURL = p.baseURL + "/media/" + id + "/index.m3u8"
	}
	p.mu.Unlock()
	_ = os.Remove(p.fullPath(id))
	if revert {
		p.emitUpdate(id)
	}
}

func (p *Pipeline) emitUpdate(id string) {
	p.mu.Lock()
	t, ok := p.tracks[id]
	fn := p.notify[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	snap := *t
	p.mu.Unlock()
	if fn != nil {
		fn(snap)
	}
}
