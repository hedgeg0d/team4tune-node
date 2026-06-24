package main

import (
	"log"
	"net/http"
	"os"

	"github.com/team4tune/node-server/internal/httpapi"
	"github.com/team4tune/node-server/internal/media"
	"github.com/team4tune/node-server/internal/room"
	"github.com/team4tune/node-server/internal/udpclock"
	"github.com/team4tune/node-server/internal/wsapi"
)

func main() {
	addr := os.Getenv("TEAM4TUNE_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	baseURL := os.Getenv("TEAM4TUNE_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	cacheDir := os.Getenv("TEAM4TUNE_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = "./cache"
	}
	udpAddr := os.Getenv("TEAM4TUNE_UDP_ADDR")
	if udpAddr == "" {
		udpAddr = ":8081"
	}

	pipeline, err := media.New(cacheDir, baseURL)
	if err != nil {
		log.Fatal(err)
	}
	reg := room.NewRegistry()
	reg.SetTrackCleaner(func(id string) { _ = pipeline.Delete(id) })
	reg.SetCacheManager(pipeline)
	reg.SetStreamResolver(pipeline)

	if udp, err := udpclock.Listen(udpAddr); err != nil {
		log.Printf("udp clock disabled: %v", err)
	} else {
		reg.SetUDPPort(udp.Port())
		log.Printf("udp clock on %s (port %d)", udpAddr, udp.Port())
	}

	mux := http.NewServeMux()
	mux.Handle("/ws", wsapi.New(reg, pipeline))
	mux.HandleFunc("/media/", pipeline.ServeMedia)
	mux.Handle("/upload", httpapi.Upload(pipeline))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("team4tune-node-server listening on %s (cache %s)", addr, cacheDir)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
