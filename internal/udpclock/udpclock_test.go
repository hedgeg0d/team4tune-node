package udpclock

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

func TestUDPEcho(t *testing.T) {
	srv, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	conn, err := net.Dial("udp", net.JoinHostPort("127.0.0.1", itoa(srv.Port())))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ping, _ := json.Marshal(protocol.PingData{T0: 12345})
	if _, err := conn.Write(ping); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	var pong protocol.PongData
	if err := json.Unmarshal(buf[:n], &pong); err != nil {
		t.Fatal(err)
	}
	if pong.T0 != 12345 {
		t.Fatalf("pong t0 = %d, want 12345", pong.T0)
	}
	if pong.T1 == 0 || pong.T2 == 0 || pong.T2 < pong.T1 {
		t.Fatalf("bad server timestamps: %+v", pong)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
