package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/team4tune/node-server/internal/media"
)

const maxUploadBytes = 100 << 20

type uploadResponse struct {
	SourceURL  string `json:"sourceUrl"`
	Title      string `json:"title"`
	DurationMs int64  `json:"durationMs"`
}

func Upload(p *media.Pipeline) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
		file, header, err := r.FormFile("file")
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "missing file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		title := r.FormValue("title")
		if title == "" && header != nil {
			title = header.Filename
		}

		track, err := p.RegisterOpus(file, title)
		if err != nil {
			if errors.Is(err, media.ErrNotOpus) {
				http.Error(w, "not an opus file", http.StatusUnsupportedMediaType)
				return
			}
			http.Error(w, "upload failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(uploadResponse{
			SourceURL:  track.SourceURL,
			Title:      track.Title,
			DurationMs: track.DurationMs,
		})
	}
}
