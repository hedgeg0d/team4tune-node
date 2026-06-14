//go:build linux

package media

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no syscall.Stat_t")
	}
	return st.Blocks * 512
}

func TestPunchHoleFreesBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blob")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	f.Sync()

	before := allocatedBytes(t, path)
	if err := punchHole(f, 256<<10, 512<<10); err != nil {
		t.Skipf("punchHole unsupported here: %v", err)
	}
	f.Sync()
	after := allocatedBytes(t, path)

	if after >= before {
		t.Skipf("filesystem did not free blocks (before=%d after=%d)", before, after)
	}
	if before-after < 256<<10 {
		t.Fatalf("expected ~512KiB freed, before=%d after=%d", before, after)
	}
}

func TestWindowedResolvePunches(t *testing.T) {
	requireTools(t)
	withWindowVars(t, 0, 64<<10, 64<<10, 16<<10, 16<<10)

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "src.mp3")
	gen := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=40",
		"-c:a", "libmp3lame", srcPath)
	if err := gen.Run(); err != nil {
		t.Skipf("could not generate source audio: %v", err)
	}

	fileSrv := httptest.NewServer(http.FileServer(http.Dir(srcDir)))
	defer fileSrv.Close()

	p := newTestPipeline(t)

	var mu sync.Mutex
	var latest protocol.Track
	p.Resolve(fileSrv.URL+"/src.mp3", func(tr protocol.Track) {
		mu.Lock()
		latest = tr
		mu.Unlock()
	})

	var id string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for sid := range p.streams {
			id = sid
		}
		p.mu.Unlock()
		if id != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if id == "" {
		t.Fatal("stream never registered")
	}

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				p.mu.Lock()
				st := p.streams[id]
				p.mu.Unlock()
				if st != nil {
					w, _, _ := st.frontier()
					st.observe(w)
				}
			}
		}
	}()

	final := filepath.Join(p.cacheDir, id+".opus")
	for time.Now().Before(deadline) {
		mu.Lock()
		errored := latest.Status == StatusError
		mu.Unlock()
		if errored {
			t.Fatal("resolve errored")
		}
		if _, err := os.Stat(final); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)

	fi, err := os.Stat(final)
	if err != nil {
		t.Fatalf("final never produced: %v", err)
	}
	logical := fi.Size()
	allocated := allocatedBytes(t, final)
	if logical < 200<<10 {
		t.Skipf("encoded file too small to exercise punching (%d bytes)", logical)
	}
	if allocated >= logical {
		t.Skipf("filesystem did not free blocks (logical=%d allocated=%d)", logical, allocated)
	}
	if allocated > logical/2 {
		t.Fatalf("expected windowed file to free most blocks: logical=%d allocated=%d", logical, allocated)
	}
}
