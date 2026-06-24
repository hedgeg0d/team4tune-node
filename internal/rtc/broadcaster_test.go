package rtc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/team4tune/node-server/internal/protocol"
)

func TestBroadcasterDeliversAudio(t *testing.T) {
	clientPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clientPC.Close() }()

	gotTrack := make(chan *webrtc.TrackRemote, 1)
	clientPC.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case gotTrack <- tr:
		default:
		}
	})

	var bc *Broadcaster
	send := func(clientID string, env protocol.Envelope) {
		var sig protocol.RTCSignal
		if err := json.Unmarshal(env.Data, &sig); err != nil {
			return
		}
		switch sig.Kind {
		case protocol.RTCOffer:
			if err := clientPC.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sig.SDP}); err != nil {
				return
			}
			answer, err := clientPC.CreateAnswer(nil)
			if err != nil {
				return
			}
			if err := clientPC.SetLocalDescription(answer); err != nil {
				return
			}
			bc.HandleSignal(clientID, protocol.RTCSignal{Kind: protocol.RTCAnswer, SDP: answer.SDP})
		case protocol.RTCICE:
			var init webrtc.ICECandidateInit
			if err := json.Unmarshal(sig.Candidate, &init); err != nil {
				return
			}
			_ = clientPC.AddICECandidate(init)
		}
	}

	bc, err = New(send, func(protocol.Track) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	clientPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		raw, err := json.Marshal(c.ToJSON())
		if err != nil {
			return
		}
		bc.HandleSignal("c1", protocol.RTCSignal{Kind: protocol.RTCICE, Candidate: raw})
	})

	bc.HandleSignal("c1", protocol.RTCSignal{Kind: protocol.RTCJoin})

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		payload := []byte{0xf8, 0xff, 0xfe}
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_ = bc.track.WriteSample(media.Sample{Data: payload, Duration: 20 * time.Millisecond})
			}
		}
	}()

	var tr *webrtc.TrackRemote
	select {
	case tr = <-gotTrack:
	case <-time.After(15 * time.Second):
		t.Fatal("client never received the broadcast track")
	}

	_ = tr.SetReadDeadline(time.Now().Add(10 * time.Second))
	pkt, _, err := tr.ReadRTP()
	if err != nil {
		t.Fatalf("read rtp: %v", err)
	}
	if len(pkt.Payload) == 0 {
		t.Fatal("empty rtp payload")
	}
}
