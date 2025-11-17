package server

import (
	"advafaeian/go-event-broker/internal/handler"
	"fmt"
	"log"
	"net"
)

type Server struct {
	addr string
}

func New(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}
	defer l.Close()

	log.Printf("Server listening on %s", s.addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handler.HandleConnection(conn)
	}
}
