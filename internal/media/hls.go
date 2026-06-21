package media

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/team4tune/node-server/internal/protocol"
)

const (
	hlsDurationThresholdMs int64 = 10 * 60 * 1000
	hlsSegmentMs           int64 = 6000
	hlsAudioBitrate              = "128k"
	hlsReadAhead           int64 = 4
	hlsSegConcurrency            = 5
)

type hlsJob struct {
	done chan struct{}
	err  error
}

func (p *Pipeline) serveHLSPlaylist(w http.ResponseWriter, r *http.Request, id string) {
	track, ok := p.Get(id)
	if !ok || track.DurationMs <= 0 || strings.HasPrefix(track.SourceURL, uploadPrefix) {
		http.NotFound(w, r)
		return
	}
	count := (track.DurationMs + hlsSegmentMs - 1) / hlsSegmentMs
	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	b.WriteString("#EXT-X-TARGETDURATION:")
	b.WriteString(strconv.FormatInt((hlsSegmentMs+999)/1000, 10))
	b.WriteByte('\n')
	b.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")
	for i := int64(0); i < count; i++ {
		dur := hlsSegmentMs
		if remain := track.DurationMs - i*hlsSegmentMs; remain < dur {
			dur = remain
		}
		b.WriteString("#EXTINF:")
		b.WriteString(formatHLSDuration(dur))
		b.WriteString(",\n")
		b.WriteString("seg/")
		b.WriteString(strconv.FormatInt(i, 10))
		b.WriteString(".ts\n")
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write([]byte(b.String()))
}

func (p *Pipeline) serveHLSSegment(w http.ResponseWriter, r *http.Request, id string, segment int64) {
	track, ok := p.Get(id)
	if !ok || track.DurationMs <= 0 || strings.HasPrefix(track.SourceURL, uploadPrefix) {
		http.NotFound(w, r)
		return
	}
	count := (track.DurationMs + hlsSegmentMs - 1) / hlsSegmentMs
	if segment < 0 || segment >= count {
		http.NotFound(w, r)
		return
	}
	path := p.hlsSegmentPath(id, segment)
	if _, err := os.Stat(path); err != nil {
		if err := p.ensureHLSSegment(track, segment); err != nil {
			http.NotFound(w, r)
			return
		}
	}
	go p.warmHLSSegments(track, segment+1, hlsReadAhead)
	f, err := os.Open(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "video/mp2t")
	http.ServeContent(w, r, filepath.Base(path), fi.ModTime(), f)
}

func (p *Pipeline) ensureHLSSegment(track protocol.Track, segment int64) error {
	if !p.exists(track.ID) {
		return os.ErrNotExist
	}
	key := track.ID + ":" + strconv.FormatInt(segment, 10)
	p.mu.Lock()
	if job := p.hlsJobs[key]; job != nil {
		p.mu.Unlock()
		<-job.done
		return job.err
	}
	job := &hlsJob{done: make(chan struct{})}
	p.hlsJobs[key] = job
	p.mu.Unlock()
	p.segSem <- struct{}{}
	job.err = p.generateHLSSegment(track, segment)
	if job.err != nil {
		p.clearDirectURL(track.ID)
		job.err = p.generateHLSSegment(track, segment)
	}
	<-p.segSem
	close(job.done)
	p.mu.Lock()
	delete(p.hlsJobs, key)
	p.mu.Unlock()
	return job.err
}

func (p *Pipeline) warmHLSSegments(track protocol.Track, from, n int64) {
	if !p.exists(track.ID) {
		return
	}
	count := (track.DurationMs + hlsSegmentMs - 1) / hlsSegmentMs
	for i := from; i < from+n && i < count; i++ {
		if _, err := os.Stat(p.hlsSegmentPath(track.ID, i)); err == nil {
			continue
		}
		go func(seg int64) { _ = p.ensureHLSSegment(track, seg) }(i)
	}
}

func (p *Pipeline) hlsSegmentPath(id string, segment int64) string {
	return filepath.Join(p.cacheDir, id+".hls", fmt.Sprintf("seg%06d.ts", segment))
}

func (p *Pipeline) generateHLSSegment(track protocol.Track, segment int64) error {
	if !p.exists(track.ID) {
		return os.ErrNotExist
	}
	dir := filepath.Join(p.cacheDir, track.ID+".hls")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := p.hlsSegmentPath(track.ID, segment)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	direct := p.directURLFor(track.ID, track.SourceURL)
	if direct == "" {
		return os.ErrNotExist
	}
	startMs := segment * hlsSegmentMs
	durMs := hlsSegmentMs
	if remain := track.DurationMs - startMs; remain < durMs {
		durMs = remain
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	if err := runHLSSegmentFFmpeg(direct, startMs, durMs, tmp, true); err != nil {
		_ = os.Remove(tmp)
		if err := runHLSSegmentFFmpeg(direct, startMs, durMs, tmp, false); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	if !p.exists(track.ID) {
		_ = os.Remove(tmp)
		_ = os.Remove(dir)
		return os.ErrNotExist
	}
	return os.Rename(tmp, path)
}

func runHLSSegmentFFmpeg(direct string, startMs, durMs int64, out string, copyAudio bool) error {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-user_agent", segUserAgent,
		"-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
		"-ss", formatFFmpegSeconds(startMs),
		"-i", direct,
		"-t", formatFFmpegSeconds(durMs),
		"-vn",
	}
	if copyAudio {
		args = append(args, "-c:a", "copy")
	} else {
		args = append(args, "-c:a", "aac", "-b:a", hlsAudioBitrate, "-ar", "48000", "-ac", "2")
	}
	args = append(args,
		"-muxdelay", "0", "-muxpreload", "0",
		"-output_ts_offset", formatFFmpegSeconds(startMs),
		"-f", "mpegts", out,
	)
	return exec.Command("ffmpeg", args...).Run()
}

func formatHLSDuration(ms int64) string {
	return fmt.Sprintf("%.3f", float64(ms)/1000)
}

func formatFFmpegSeconds(ms int64) string {
	return fmt.Sprintf("%.3f", float64(ms)/1000)
}
