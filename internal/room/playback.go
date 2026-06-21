package room

import (
	"slices"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

const (
	startBufferMs          = 2000
	responsiveReadyTimeout = 6 * time.Second
	tightReadyTimeout      = 20 * time.Second
	endGraceMs             = 750
)

func (room *Room) readyDeadline() time.Duration {
	if room.settings.Sync == protocol.SyncTight {
		return tightReadyTimeout
	}
	return responsiveReadyTimeout
}

type playback struct {
	trackID    string
	fileURL    string
	title      string
	durationMs int64
	ready      map[string]bool
	started    bool
	paused     bool
	t0         int64
	s          int64
	readyTimer *time.Timer
	endTimer   *time.Timer
}

func nowMs() int64 { return time.Now().UnixMilli() }

func nowPlayingEnv(pb *playback) protocol.Envelope {
	return protocol.MustEncode(protocol.TypeNowPlaying, protocol.NowPlayingData{
		TrackID: pb.trackID,
		T0:      pb.t0,
		S:       pb.s,
		Paused:  pb.paused,
	})
}

func (room *Room) scheduleEnd(pb *playback) {
	if pb.endTimer != nil {
		pb.endTimer.Stop()
		pb.endTimer = nil
	}
	if pb.paused || pb.durationMs <= 0 {
		return
	}
	remaining := pb.durationMs - pb.s
	if remaining < 0 {
		remaining = 0
	}
	trackID := pb.trackID
	wait := time.Duration(pb.t0-nowMs()+remaining+endGraceMs) * time.Millisecond
	pb.endTimer = time.AfterFunc(wait, func() { room.endTrack(trackID) })
}

func (room *Room) sendAll(env protocol.Envelope) {
	room.mu.Lock()
	chans := make([]chan protocol.Envelope, 0, len(room.clients))
	for _, c := range room.clients {
		chans = append(chans, c.Send)
	}
	room.mu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- env:
		default:
		}
	}
}

func (room *Room) maybeStart() {
	room.mu.Lock()
	if room.playing != nil {
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
	pb := &playback{
		trackID:    chosen.ID,
		fileURL:    chosen.FileURL,
		title:      chosen.Title,
		durationMs: chosen.DurationMs,
		ready:      map[string]bool{},
	}
	pb.readyTimer = time.AfterFunc(room.readyDeadline(), func() { room.startPlayback(pb.trackID) })
	room.playing = pb
	prepare := protocol.MustEncode(protocol.TypePrepare, protocol.PrepareData{
		TrackID:    pb.trackID,
		FileURL:    pb.fileURL,
		DurationMs: pb.durationMs,
		Title:      pb.title,
	})
	room.mu.Unlock()

	room.sendAll(prepare)
}

func (room *Room) MarkReady(clientID, trackID string) {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || pb.trackID != trackID {
		room.mu.Unlock()
		return
	}
	if pb.started {
		if pb.ready[clientID] {
			room.mu.Unlock()
			return
		}
		pb.ready[clientID] = true
		np := nowPlayingEnv(pb)
		room.mu.Unlock()
		room.sendTo(clientID, np)
		return
	}
	pb.ready[clientID] = true
	allReady := true
	for id := range room.clients {
		if !pb.ready[id] {
			allReady = false
			break
		}
	}
	room.mu.Unlock()

	if allReady {
		room.startPlayback(trackID)
	}
}

func (room *Room) recheckReady() {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || pb.started || len(room.clients) == 0 {
		room.mu.Unlock()
		return
	}
	trackID := pb.trackID
	allReady := true
	for id := range room.clients {
		if !pb.ready[id] {
			allReady = false
			break
		}
	}
	room.mu.Unlock()

	if allReady {
		room.startPlayback(trackID)
	}
}

func (room *Room) startPlayback(trackID string) {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || pb.trackID != trackID || pb.started {
		room.mu.Unlock()
		return
	}
	pb.started = true
	pb.t0 = nowMs() + startBufferMs
	pb.s = 0
	if pb.readyTimer != nil {
		pb.readyTimer.Stop()
	}
	room.scheduleEnd(pb)
	nowPlaying := nowPlayingEnv(pb)
	room.mu.Unlock()

	room.sendAll(nowPlaying)
}

func (room *Room) Pause() {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || !pb.started || pb.paused {
		room.mu.Unlock()
		return
	}
	pb.s += nowMs() - pb.t0
	pb.t0 = nowMs()
	pb.paused = true
	room.scheduleEnd(pb)
	np := nowPlayingEnv(pb)
	room.mu.Unlock()

	room.sendAll(np)
}

func (room *Room) Resume() {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || !pb.started || !pb.paused {
		room.mu.Unlock()
		return
	}
	pb.t0 = nowMs() + startBufferMs
	pb.paused = false
	room.scheduleEnd(pb)
	np := nowPlayingEnv(pb)
	room.mu.Unlock()

	room.sendAll(np)
}

func (room *Room) SeekTo(ms int64) {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || !pb.started {
		room.mu.Unlock()
		return
	}
	if ms < 0 {
		ms = 0
	}
	pb.s = ms
	if pb.paused {
		pb.t0 = nowMs()
	} else {
		pb.t0 = nowMs() + startBufferMs
	}
	room.scheduleEnd(pb)
	np := nowPlayingEnv(pb)
	room.mu.Unlock()

	room.sendAll(np)
}

func (room *Room) Skip() {
	room.mu.Lock()
	pb := room.playing
	if pb == nil {
		room.mu.Unlock()
		return
	}
	trackID := pb.trackID
	room.mu.Unlock()

	room.endTrack(trackID)
}

func (room *Room) endTrack(trackID string) {
	room.mu.Lock()
	pb := room.playing
	if pb == nil || pb.trackID != trackID {
		room.mu.Unlock()
		return
	}
	if pb.endTimer != nil {
		pb.endTimer.Stop()
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
	room.maybeStart()
	room.reconcileCache()
}

func (room *Room) catchUp(c *Client) {
	room.mu.Lock()
	pb := room.playing
	if pb == nil {
		room.mu.Unlock()
		return
	}
	prepare := protocol.MustEncode(protocol.TypePrepare, protocol.PrepareData{
		TrackID:    pb.trackID,
		FileURL:    pb.fileURL,
		DurationMs: pb.durationMs,
		Title:      pb.title,
	})
	var nowPlaying *protocol.Envelope
	if pb.started {
		np := nowPlayingEnv(pb)
		nowPlaying = &np
	}
	send := c.Send
	room.mu.Unlock()

	select {
	case send <- prepare:
	default:
	}
	if nowPlaying != nil {
		select {
		case send <- *nowPlaying:
		default:
		}
	}
}
