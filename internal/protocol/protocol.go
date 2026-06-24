package protocol

import "encoding/json"

type Envelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

func Encode(typ string, data any) (Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Type: typ, Data: raw}, nil
}

func MustEncode(typ string, data any) Envelope {
	env, err := Encode(typ, data)
	if err != nil {
		panic(err)
	}
	return env
}

const (
	TypeCreate      = "create"
	TypeJoin        = "join"
	TypeEnqueue     = "enqueue"
	TypeSkip        = "skip"
	TypeRemove      = "remove"
	TypeControl     = "control"
	TypeSetSettings = "set_settings"
	TypeReady       = "ready"
	TypeProgress    = "progress"
	TypeBye         = "bye"
	TypePing        = "ping"
	TypeRTC         = "rtc"
)

const (
	TypeRoomState  = "room_state"
	TypeNowPlaying = "now_playing"
	TypePrepare    = "prepare"
	TypePong       = "pong"
	TypeError      = "error"
)

type RoomMode string

const (
	ModeSignal RoomMode = "signal"
	ModeStream RoomMode = "stream"
)

const (
	StatusResolving = "resolving"
	StatusReady     = "ready"
	StatusError     = "error"
)

type Policy string

const (
	PolicyEveryone Policy = "everyone"
	PolicyHost     Policy = "host"
)

type SyncMode string

const (
	SyncResponsive SyncMode = "responsive"
	SyncTight      SyncMode = "tight"
)

const (
	ScopeEnqueue = "enqueue"
	ScopeSkip    = "skip"
	ScopeRemove  = "remove"
	ScopeControl = "control"
)

const (
	ControlPause  = "pause"
	ControlResume = "resume"
	ControlSeek   = "seek"
)

const (
	MemLimitMinMB     = 2
	MemLimitMaxMB     = 100
	MemLimitDefaultMB = 50
)

const (
	StreamBitrateMinKbps     = 16
	StreamBitrateMaxKbps     = 256
	StreamBitrateDefaultKbps = 64
)

type RoomSettings struct {
	Enqueue           Policy   `json:"enqueue"`
	Skip              Policy   `json:"skip"`
	Remove            Policy   `json:"remove"`
	Control           Policy   `json:"control"`
	Sync              SyncMode `json:"sync"`
	MemLimitMB        int      `json:"memLimitMb"`
	StreamBitrateKbps int      `json:"streamBitrateKbps"`
}

func DefaultSettings() RoomSettings {
	return RoomSettings{
		Enqueue:           PolicyEveryone,
		Skip:              PolicyEveryone,
		Remove:            PolicyEveryone,
		Control:           PolicyEveryone,
		Sync:              SyncResponsive,
		MemLimitMB:        MemLimitDefaultMB,
		StreamBitrateKbps: StreamBitrateDefaultKbps,
	}
}

func (s RoomSettings) PolicyFor(scope string) Policy {
	switch scope {
	case ScopeEnqueue:
		return s.Enqueue
	case ScopeSkip:
		return s.Skip
	case ScopeRemove:
		return s.Remove
	case ScopeControl:
		return s.Control
	default:
		return PolicyHost
	}
}

type CreateData struct {
	Mode RoomMode `json:"mode"`
	Nick string   `json:"nick"`
}

type JoinData struct {
	RoomCode    string `json:"roomCode"`
	Nick        string `json:"nick"`
	ResumeToken string `json:"resumeToken,omitempty"`
}

type ProgressData struct {
	TrackID    string `json:"trackId"`
	HaveMs     int64  `json:"haveMs"`
	TotalMs    int64  `json:"totalMs"`
	Bps        int64  `json:"bps"`
	Ready      bool   `json:"ready"`
	Confidence int    `json:"confidence"`
}

type EnqueueData struct {
	SourceURL string `json:"sourceUrl"`
}

type RemoveData struct {
	TrackID string `json:"trackId"`
}

type ControlData struct {
	Action string `json:"action"`
	SeekMs int64  `json:"seekMs,omitempty"`
}

type PingData struct {
	T0 int64 `json:"t0"`
}

const (
	RTCJoin   = "join"
	RTCOffer  = "offer"
	RTCAnswer = "answer"
	RTCICE    = "ice"
)

type RTCSignal struct {
	Kind      string          `json:"kind"`
	SDP       string          `json:"sdp,omitempty"`
	Candidate json.RawMessage `json:"candidate,omitempty"`
}

type Health struct {
	Confidence int   `json:"confidence"`
	BufferedMs int64 `json:"bufferedMs"`
	Bps        int64 `json:"bps"`
	Ready      bool  `json:"ready"`
}

type Member struct {
	ID     string  `json:"id"`
	Nick   string  `json:"nick"`
	Health *Health `json:"health,omitempty"`
}

type RoomStateData struct {
	RoomCode    string       `json:"roomCode"`
	Mode        RoomMode     `json:"mode"`
	SelfID      string       `json:"selfId"`
	HostID      string       `json:"hostId"`
	Settings    RoomSettings `json:"settings"`
	Members     []Member     `json:"members"`
	Queue       []Track      `json:"queue"`
	Playing     string       `json:"playingTrackId,omitempty"`
	UDPPort     int          `json:"udpPort,omitempty"`
	ResumeToken string       `json:"resumeToken,omitempty"`
}

type Track struct {
	ID         string `json:"id"`
	SourceURL  string `json:"sourceUrl"`
	Status     string `json:"status"`
	Title      string `json:"title,omitempty"`
	FileURL    string `json:"fileUrl,omitempty"`
	DurationMs int64  `json:"durationMs,omitempty"`
}

type ReadyData struct {
	TrackID string `json:"trackId"`
}

type PrepareData struct {
	TrackID    string `json:"trackId"`
	FileURL    string `json:"fileUrl"`
	DurationMs int64  `json:"durationMs,omitempty"`
	Title      string `json:"title,omitempty"`
}

type NowPlayingData struct {
	TrackID string `json:"trackId"`
	T0      int64  `json:"t0"`
	S       int64  `json:"s"`
	Paused  bool   `json:"paused"`
}

type PongData struct {
	T0 int64 `json:"t0"`
	T1 int64 `json:"t1"`
	T2 int64 `json:"t2"`
}

type ErrorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
