package media

import (
	"os"
	"testing"

	"github.com/team4tune/node-server/internal/protocol"
)

func TestPlanFullBudget(t *testing.T) {
	long := hlsDurationThresholdMs
	tracks := []ReconcileTrack{
		{ID: "a", SourceURL: "https://x/a", DurationMs: long},
		{ID: "b", SourceURL: "https://x/b", DurationMs: long},
		{ID: "c", SourceURL: uploadPrefix + "c", DurationMs: long},
		{ID: "d", SourceURL: "https://x/d", DurationMs: 60_000},
	}

	tight := planFull(tracks, 10)
	if !tight["a"] {
		t.Fatalf("a should be full within budget")
	}
	if tight["b"] {
		t.Fatalf("b should not fit a 10MB budget after a")
	}
	if _, ok := tight["c"]; ok {
		t.Fatalf("upload track must not be in the full plan")
	}
	if _, ok := tight["d"]; ok {
		t.Fatalf("short track must not be in the full plan")
	}

	roomy := planFull(tracks, 50)
	if !roomy["a"] || !roomy["b"] {
		t.Fatalf("both long tracks should fit a 50MB budget")
	}
}

func TestEvictFullRemovesFileAndReverts(t *testing.T) {
	p := newTestPipeline(t)
	id := "track1"
	full := p.baseURL + "/media/" + id + ".m4a"
	p.tracks[id] = &protocol.Track{ID: id, Status: StatusReady, FileURL: full}
	p.fullDesired[id] = true
	writeFile(t, p.fullPath(id))

	p.evictFull(id)

	if _, err := os.Stat(p.fullPath(id)); !os.IsNotExist(err) {
		t.Fatalf("m4a file should be removed")
	}
	if got := p.tracks[id].FileURL; got != p.baseURL+"/media/"+id+"/index.m3u8" {
		t.Fatalf("FileURL should revert to m3u8, got %q", got)
	}
	if p.fullDesired[id] {
		t.Fatalf("fullDesired should be cleared")
	}
}

func TestSetFullReadyDropsOrphan(t *testing.T) {
	p := newTestPipeline(t)
	id := "track2"
	parts := p.baseURL + "/media/" + id + "/index.m3u8"
	p.tracks[id] = &protocol.Track{ID: id, Status: StatusReady, FileURL: parts}
	p.fullDesired[id] = false
	writeFile(t, p.fullPath(id))

	p.setFullReady(id)

	if _, err := os.Stat(p.fullPath(id)); !os.IsNotExist(err) {
		t.Fatalf("orphan m4a should be removed when full is no longer desired")
	}
	if p.tracks[id].FileURL != parts {
		t.Fatalf("FileURL must not upgrade when full is not desired")
	}
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}
