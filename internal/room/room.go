package room

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

var ErrRoomNotFound = errors.New("room not found")

const (
	resumeTTL    = 60 * time.Second
	emptyRoomTTL = 2 * time.Minute
)

type Client struct {
	ID    string
	Nick  string
	Seq   int64
	Token string
	Send  chan protocol.Envelope
}

type pendingClient struct {
	seq   int64
	token string
	nick  string
	timer *time.Timer
}

type Room struct {
	Code string
	Mode protocol.RoomMode

	mu         sync.Mutex
	clients    map[string]*Client
	queue      []protocol.Track
	playing    *playback
	hostID     string
	seqCounter int64
	settings   protocol.RoomSettings
	health     map[string]*protocol.Health
	udpPort    int
	tokens     map[string]string
	pending    map[string]*pendingClient
	emptyTimer *time.Timer
	cleanTrack func(string)
}

type Registry struct {
	mu           sync.Mutex
	rooms        map[string]*Room
	udpPort      int
	cleanTrack   func(string)
	emptyRoomTTL time.Duration
}

func NewRegistry() *Registry {
	return &Registry{rooms: make(map[string]*Room), emptyRoomTTL: emptyRoomTTL}
}

func (r *Registry) SetUDPPort(port int) {
	r.mu.Lock()
	r.udpPort = port
	r.mu.Unlock()
}

func (r *Registry) SetTrackCleaner(fn func(string)) {
	r.mu.Lock()
	r.cleanTrack = fn
	r.mu.Unlock()
}

func (r *Registry) Create(mode protocol.RoomMode) *Room {
	r.mu.Lock()
	defer r.mu.Unlock()

	var code string
	for {
		code = genCode()
		if _, exists := r.rooms[code]; !exists {
			break
		}
	}

	room := &Room{
		Code:       code,
		Mode:       mode,
		clients:    make(map[string]*Client),
		settings:   protocol.DefaultSettings(),
		health:     make(map[string]*protocol.Health),
		udpPort:    r.udpPort,
		tokens:     make(map[string]string),
		pending:    make(map[string]*pendingClient),
		cleanTrack: r.cleanIfUnused,
	}
	r.rooms[code] = room
	return room
}

func (r *Registry) Get(code string) (*Room, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room, ok := r.rooms[code]
	if !ok {
		return nil, ErrRoomNotFound
	}
	return room, nil
}

func (r *Registry) remove(code string) {
	r.mu.Lock()
	room := r.rooms[code]
	delete(r.rooms, code)
	r.mu.Unlock()
	if room != nil {
		room.cleanupMedia()
	}
}

func (r *Registry) cleanIfUnused(trackID string) {
	r.mu.Lock()
	rooms := make([]*Room, 0, len(r.rooms))
	clean := r.cleanTrack
	for _, room := range r.rooms {
		rooms = append(rooms, room)
	}
	r.mu.Unlock()
	if clean == nil {
		return
	}
	for _, room := range rooms {
		if room.referencesTrack(trackID) {
			return
		}
	}
	clean(trackID)
}

func (room *Room) referencesTrack(trackID string) bool {
	room.mu.Lock()
	defer room.mu.Unlock()
	if room.playing != nil && room.playing.trackID == trackID {
		return true
	}
	for _, t := range room.queue {
		if t.ID == trackID {
			return true
		}
	}
	return false
}

func (room *Room) stopEmptyTimerLocked() {
	if room.emptyTimer != nil {
		room.emptyTimer.Stop()
		room.emptyTimer = nil
	}
}

func (room *Room) scheduleEmptyTimerLocked(reg *Registry, empty bool) {
	if !empty {
		room.stopEmptyTimerLocked()
		return
	}
	if room.emptyTimer != nil {
		return
	}
	room.emptyTimer = time.AfterFunc(reg.emptyRoomTTL, func() {
		room.mu.Lock()
		empty := len(room.clients) == 0
		room.mu.Unlock()
		if empty {
			reg.remove(room.Code)
		}
	})
}

func (room *Room) cleanupMedia() {
	room.mu.Lock()
	room.stopEmptyTimerLocked()
	ids := make(map[string]struct{})
	if room.playing != nil {
		ids[room.playing.trackID] = struct{}{}
		if room.playing.readyTimer != nil {
			room.playing.readyTimer.Stop()
		}
		if room.playing.endTimer != nil {
			room.playing.endTimer.Stop()
		}
	}
	for _, t := range room.queue {
		ids[t.ID] = struct{}{}
	}
	for _, p := range room.pending {
		if p.timer != nil {
			p.timer.Stop()
		}
	}
	room.playing = nil
	room.queue = nil
	clean := room.cleanTrack
	room.mu.Unlock()
	if clean == nil {
		return
	}
	for id := range ids {
		clean(id)
	}
}

func (room *Room) cleanTrackFile(trackID string) {
	clean := room.cleanTrack
	if clean != nil {
		clean(trackID)
	}
}

func (room *Room) Join(c *Client, resumeToken string) {
	room.mu.Lock()
	room.stopEmptyTimerLocked()
	if resumeToken != "" {
		if oldID, ok := room.tokens[resumeToken]; ok {
			c.ID = oldID
			c.Token = resumeToken
			if p, ok := room.pending[oldID]; ok {
				c.Seq = p.seq
				if c.Nick == "" {
					c.Nick = p.nick
				}
				if p.timer != nil {
					p.timer.Stop()
				}
				delete(room.pending, oldID)
			} else {
				room.seqCounter++
				c.Seq = room.seqCounter
			}
			room.clients[c.ID] = c
			if room.hostID == "" {
				room.hostID = c.ID
			}
			room.mu.Unlock()
			room.broadcastState()
			room.catchUp(c)
			return
		}
	}

	room.seqCounter++
	c.Seq = room.seqCounter
	c.Token = genToken()
	room.tokens[c.Token] = c.ID
	room.clients[c.ID] = c
	if room.hostID == "" {
		room.hostID = c.ID
	}
	room.mu.Unlock()
	room.broadcastState()
	room.catchUp(c)
}

func (room *Room) Leave(reg *Registry, c *Client) {
	room.mu.Lock()
	if room.clients[c.ID] != c {
		room.mu.Unlock()
		return
	}
	clientID := c.ID
	delete(room.clients, clientID)
	if c.Token != "" {
		room.pending[clientID] = &pendingClient{
			seq:   c.Seq,
			token: c.Token,
			nick:  c.Nick,
			timer: time.AfterFunc(resumeTTL, func() { room.finalizeLeave(reg, clientID) }),
		}
		room.scheduleEmptyTimerLocked(reg, len(room.clients) == 0)
		room.mu.Unlock()
		room.broadcastState()
		room.recheckReady()
		return
	}
	empty := len(room.clients) == 0
	room.promoteIfHostGone(clientID)
	room.scheduleEmptyTimerLocked(reg, empty)
	room.mu.Unlock()

	room.broadcastState()
	room.recheckReady()
}

func (room *Room) HardLeave(reg *Registry, clientID string) {
	room.mu.Lock()
	if c := room.clients[clientID]; c != nil {
		delete(room.tokens, c.Token)
	}
	delete(room.clients, clientID)
	delete(room.health, clientID)
	if p := room.pending[clientID]; p != nil {
		if p.timer != nil {
			p.timer.Stop()
		}
		delete(room.tokens, p.token)
		delete(room.pending, clientID)
	}
	room.promoteIfHostGone(clientID)
	room.scheduleEmptyTimerLocked(reg, len(room.clients) == 0)
	room.mu.Unlock()

	room.broadcastState()
	room.recheckReady()
}

func (room *Room) finalizeLeave(reg *Registry, clientID string) {
	room.mu.Lock()
	p := room.pending[clientID]
	if p == nil {
		room.mu.Unlock()
		return
	}
	delete(room.pending, clientID)
	delete(room.tokens, p.token)
	delete(room.health, clientID)
	room.promoteIfHostGone(clientID)
	room.scheduleEmptyTimerLocked(reg, len(room.clients) == 0)
	room.mu.Unlock()

	room.broadcastState()
	room.recheckReady()
}

func (room *Room) promoteIfHostGone(clientID string) {
	if room.hostID != clientID {
		return
	}
	var oldest *Client
	for _, c := range room.clients {
		if oldest == nil || c.Seq < oldest.Seq {
			oldest = c
		}
	}
	if oldest != nil {
		room.hostID = oldest.ID
	}
}

func genToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func (room *Room) IsAllowed(clientID, scope string) bool {
	room.mu.Lock()
	defer room.mu.Unlock()
	if room.settings.PolicyFor(scope) == protocol.PolicyEveryone {
		return true
	}
	return clientID == room.hostID
}

func (room *Room) IsHost(clientID string) bool {
	room.mu.Lock()
	defer room.mu.Unlock()
	return clientID == room.hostID
}

func (room *Room) SetSettings(s protocol.RoomSettings) {
	if s.Sync != protocol.SyncResponsive && s.Sync != protocol.SyncTight {
		s.Sync = protocol.SyncResponsive
	}
	room.mu.Lock()
	room.settings = s
	room.mu.Unlock()
	room.broadcastState()
}

func (room *Room) sendTo(clientID string, env protocol.Envelope) {
	room.mu.Lock()
	c := room.clients[clientID]
	room.mu.Unlock()
	if c == nil {
		return
	}
	select {
	case c.Send <- env:
	default:
	}
}

func (room *Room) MarkProgress(clientID string, d protocol.ProgressData) {
	room.mu.Lock()
	room.health[clientID] = &protocol.Health{
		Confidence: d.Confidence,
		BufferedMs: d.HaveMs,
		Bps:        d.Bps,
		Ready:      d.Ready,
	}
	room.mu.Unlock()
	room.broadcastState()
	if d.Ready {
		room.MarkReady(clientID, d.TrackID)
	}
}

func (room *Room) RemoveTrack(trackID string) {
	room.mu.Lock()
	if room.playing != nil && room.playing.trackID == trackID {
		room.mu.Unlock()
		return
	}
	for i := range room.queue {
		if room.queue[i].ID == trackID {
			room.queue = slices.Delete(room.queue, i, i+1)
			break
		}
	}
	room.mu.Unlock()
	room.broadcastState()
}

func (room *Room) AddTrack(t protocol.Track) {
	room.mu.Lock()
	room.queue = append(room.queue, t)
	room.mu.Unlock()
	room.broadcastState()
	room.maybeStart()
}

func (room *Room) UpsertTrack(t protocol.Track) {
	room.mu.Lock()
	found := false
	for i := range room.queue {
		if room.queue[i].ID == t.ID {
			room.queue[i] = t
			found = true
			break
		}
	}
	if !found {
		room.queue = append(room.queue, t)
	}
	room.mu.Unlock()
	room.broadcastState()
	room.maybeStart()
}

func (room *Room) snapshot(selfID string) protocol.RoomStateData {
	members := make([]protocol.Member, 0, len(room.clients)+len(room.pending))
	for _, c := range room.clients {
		members = append(members, protocol.Member{ID: c.ID, Nick: c.Nick, Health: room.health[c.ID]})
	}
	for id, p := range room.pending {
		members = append(members, protocol.Member{ID: id, Nick: p.nick, Health: room.health[id]})
	}
	queue := make([]protocol.Track, len(room.queue))
	copy(queue, room.queue)
	data := protocol.RoomStateData{
		RoomCode: room.Code,
		Mode:     room.Mode,
		SelfID:   selfID,
		HostID:   room.hostID,
		Settings: room.settings,
		Members:  members,
		Queue:    queue,
		UDPPort:  room.udpPort,
	}
	if c := room.clients[selfID]; c != nil {
		data.ResumeToken = c.Token
	}
	return data
}

func (room *Room) broadcastState() {
	room.mu.Lock()
	defer room.mu.Unlock()
	for id, c := range room.clients {
		state := room.snapshot(id)
		env := protocol.MustEncode(protocol.TypeRoomState, state)
		select {
		case c.Send <- env:
		default:
		}
	}
}

func genCode() string {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
