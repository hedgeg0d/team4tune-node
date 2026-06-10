package udpclock

import (
	"encoding/json"
	"net"
	"time"

	"github.com/team4tune/node-server/internal/protocol"
)

type Server struct {
	conn *net.UDPConn
}

func Listen(addr string) (*Server, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	s := &Server{conn: conn}
	go s.loop()
	return s, nil
}

func (s *Server) Port() int {
	return s.conn.LocalAddr().(*net.UDPAddr).Port
}

func (s *Server) loop() {
	buf := make([]byte, 512)
	for {
		n, raddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		t1 := time.Now().UnixMilli()
		var ping protocol.PingData
		if json.Unmarshal(buf[:n], &ping) != nil {
			continue
		}
		resp, err := json.Marshal(protocol.PongData{
			T0: ping.T0,
			T1: t1,
			T2: time.Now().UnixMilli(),
		})
		if err != nil {
			continue
		}
		_, _ = s.conn.WriteToUDP(resp, raddr)
	}
}

func (s *Server) Close() error { return s.conn.Close() }
