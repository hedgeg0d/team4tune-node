package rtc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/oggreader"

	"github.com/team4tune/node-server/internal/protocol"
)

var iceServers = []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}

type SendFunc func(clientID string, env protocol.Envelope)

type ResolveFunc func(protocol.Track) (string, bool)

type peerConn struct {
	pc        *webrtc.PeerConnection
	mu        sync.Mutex
	remoteSet bool
	pending   []webrtc.ICECandidateInit
}

type Broadcaster struct {
	api     *webrtc.API
	send    SendFunc
	resolve ResolveFunc
	track   *webrtc.TrackLocalStaticSample

	mu       sync.Mutex
	peers    map[string]*peerConn
	bitrate  int
	cancel   context.CancelFunc
	paused   bool
	resumeCh chan struct{}
	closed   bool
}

func New(send SendFunc, resolve ResolveFunc) (*Broadcaster, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(ir))
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "team4tune",
	)
	if err != nil {
		return nil, err
	}
	return &Broadcaster{
		api:      api,
		send:     send,
		resolve:  resolve,
		track:    track,
		peers:    make(map[string]*peerConn),
		bitrate:  protocol.StreamBitrateDefaultKbps,
		resumeCh: make(chan struct{}),
	}, nil
}

func (b *Broadcaster) HandleSignal(clientID string, sig protocol.RTCSignal) {
	switch sig.Kind {
	case protocol.RTCJoin:
		b.addPeer(clientID)
	case protocol.RTCAnswer:
		b.onAnswer(clientID, sig.SDP)
	case protocol.RTCICE:
		b.onRemoteICE(clientID, sig.Candidate)
	}
}

func (b *Broadcaster) addPeer(clientID string) {
	b.removePeer(clientID)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	pc, err := b.api.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers})
	if err != nil {
		b.mu.Unlock()
		return
	}
	if _, err := pc.AddTrack(b.track); err != nil {
		b.mu.Unlock()
		_ = pc.Close()
		return
	}
	b.peers[clientID] = &peerConn{pc: pc}
	send := b.send
	b.mu.Unlock()

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		raw, err := json.Marshal(c.ToJSON())
		if err != nil {
			return
		}
		send(clientID, protocol.MustEncode(protocol.TypeRTC, protocol.RTCSignal{Kind: protocol.RTCICE, Candidate: raw}))
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
			b.removePeer(clientID)
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		b.removePeer(clientID)
		return
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		b.removePeer(clientID)
		return
	}
	send(clientID, protocol.MustEncode(protocol.TypeRTC, protocol.RTCSignal{Kind: protocol.RTCOffer, SDP: offer.SDP}))
}

func (b *Broadcaster) onAnswer(clientID, sdp string) {
	b.mu.Lock()
	p := b.peers[clientID]
	b.mu.Unlock()
	if p == nil {
		return
	}
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
		return
	}
	p.mu.Lock()
	p.remoteSet = true
	pending := p.pending
	p.pending = nil
	p.mu.Unlock()
	for _, c := range pending {
		_ = p.pc.AddICECandidate(c)
	}
}

func (b *Broadcaster) onRemoteICE(clientID string, raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var init webrtc.ICECandidateInit
	if err := json.Unmarshal(raw, &init); err != nil {
		return
	}
	b.mu.Lock()
	p := b.peers[clientID]
	b.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.remoteSet {
		p.pending = append(p.pending, init)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	_ = p.pc.AddICECandidate(init)
}

func (b *Broadcaster) RemovePeer(clientID string) {
	b.removePeer(clientID)
}

func (b *Broadcaster) removePeer(clientID string) {
	b.mu.Lock()
	p := b.peers[clientID]
	delete(b.peers, clientID)
	b.mu.Unlock()
	if p != nil {
		_ = p.pc.Close()
	}
}

func (b *Broadcaster) SetBitrate(kbps int) {
	b.mu.Lock()
	b.bitrate = kbps
	b.mu.Unlock()
}

func (b *Broadcaster) Play(track protocol.Track, onEnd func()) {
	b.Stop()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	bitrate := b.bitrate
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.mu.Unlock()

	go func() {
		input, ok := b.resolve(track)
		if !ok {
			b.finish(ctx, onEnd)
			return
		}
		b.pump(ctx, input, bitrate, onEnd)
	}()
}

func (b *Broadcaster) Stop() {
	b.mu.Lock()
	cancel := b.cancel
	b.cancel = nil
	b.paused = false
	ch := b.resumeCh
	b.resumeCh = make(chan struct{})
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	close(ch)
}

func (b *Broadcaster) Pause() {
	b.mu.Lock()
	b.paused = true
	b.mu.Unlock()
}

func (b *Broadcaster) Resume() {
	b.mu.Lock()
	if !b.paused {
		b.mu.Unlock()
		return
	}
	b.paused = false
	ch := b.resumeCh
	b.resumeCh = make(chan struct{})
	b.mu.Unlock()
	close(ch)
}

func (b *Broadcaster) Close() {
	b.Stop()
	b.mu.Lock()
	b.closed = true
	peers := b.peers
	b.peers = make(map[string]*peerConn)
	b.mu.Unlock()
	for _, p := range peers {
		_ = p.pc.Close()
	}
}

func (b *Broadcaster) pump(ctx context.Context, input string, bitrate int, onEnd func()) {
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-i", input,
		"-vn", "-c:a", "libopus", "-b:a", fmt.Sprintf("%dk", bitrate),
		"-ar", "48000", "-ac", "2",
		"-page_duration", "20000",
		"-f", "ogg", "pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		b.finish(ctx, onEnd)
		return
	}
	if err := cmd.Start(); err != nil {
		b.finish(ctx, onEnd)
		return
	}
	defer func() { _ = cmd.Wait() }()

	ogg, _, err := oggreader.NewWith(stdout)
	if err != nil {
		b.finish(ctx, onEnd)
		return
	}

	var lastGranule uint64
	page := 0
	for {
		if ctx.Err() != nil {
			return
		}
		if !b.waitIfPaused(ctx) {
			return
		}
		data, hdr, err := ogg.ParseNextPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		page++
		if page <= 2 {
			lastGranule = hdr.GranulePosition
			continue
		}
		sampleCount := hdr.GranulePosition - lastGranule
		lastGranule = hdr.GranulePosition
		dur := time.Duration(sampleCount) * time.Second / 48000
		if err := b.track.WriteSample(media.Sample{Data: data, Duration: dur}); err != nil {
			break
		}
		if dur > 0 {
			time.Sleep(dur)
		}
	}
	b.finish(ctx, onEnd)
}

func (b *Broadcaster) waitIfPaused(ctx context.Context) bool {
	for {
		b.mu.Lock()
		if !b.paused || b.closed {
			b.mu.Unlock()
			return ctx.Err() == nil
		}
		ch := b.resumeCh
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-ch:
		}
	}
}

func (b *Broadcaster) finish(ctx context.Context, onEnd func()) {
	if ctx.Err() != nil {
		return
	}
	if onEnd != nil {
		onEnd()
	}
}
