package room

import (
	"slices"

	"github.com/team4tune/node-server/internal/protocol"
)

type StreamResolver interface {
	StreamInput(protocol.Track) (string, bool)
}

func (r *Registry) SetStreamResolver(sr StreamResolver) {
	r.mu.Lock()
	r.streamSrc = sr
	r.mu.Unlock()
}

func (room *Room) isStream() bool { return room.Mode == protocol.ModeStream }

func (room *Room) HandleRTC(clientID string, sig protocol.RTCSignal) {
	if room.broadcaster == nil {
		return
	}
	room.broadcaster.HandleSignal(clientID, sig)
}

func (room *Room) removeStreamPeer(clientID string) {
	if room.broadcaster != nil {
		room.broadcaster.RemovePeer(clientID)
	}
}

func (room *Room) streamMaybeStart() {
	room.mu.Lock()
	if room.broadcaster == nil || room.playing != nil {
		room.mu.Unlock()
		return
	}
	var chosen *protocol.Track
	for i := range room.queue {
		if room.queue[i].Status == protocol.StatusReady {
			chosen = &room.queue[i]
			break
		}
	}
	if chosen == nil {
		room.mu.Unlock()
		return
	}
	track := *chosen
	room.playing = &playback{trackID: track.ID, title: track.Title, durationMs: track.DurationMs}
	bc := room.broadcaster
	room.mu.Unlock()

	bc.Play(track, func() { room.streamEndTrack(track.ID) })
	room.broadcastState()
	room.streamPreloadNext()
}

func (room *Room) streamEndTrack(trackID string) {
	room.mu.Lock()
	if room.playing == nil || room.playing.trackID != trackID {
		room.mu.Unlock()
		return
	}
	for i := range room.queue {
		if room.queue[i].ID == trackID {
			room.queue = slices.Delete(room.queue, i, i+1)
			break
		}
	}
	room.playing = nil
	room.mu.Unlock()

	room.cleanTrackFile(trackID)
	room.broadcastState()
	room.streamMaybeStart()
}

func (room *Room) streamSkip() {
	room.mu.Lock()
	bc := room.broadcaster
	var trackID string
	if room.playing != nil {
		trackID = room.playing.trackID
	}
	room.mu.Unlock()
	if bc == nil || trackID == "" {
		return
	}
	bc.Stop()
	room.streamEndTrack(trackID)
}

func (room *Room) streamPreloadNext() {
	room.mu.Lock()
	sr := room.streamSrc
	var next protocol.Track
	found := false
	passedPlaying := room.playing == nil
	for i := range room.queue {
		t := room.queue[i]
		if room.playing != nil && t.ID == room.playing.trackID {
			passedPlaying = true
			continue
		}
		if passedPlaying && t.Status == protocol.StatusReady {
			next = t
			found = true
			break
		}
	}
	room.mu.Unlock()
	if sr != nil && found {
		go sr.StreamInput(next)
	}
}
