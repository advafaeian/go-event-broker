package server

import (
	"advafaeian/go-event-broker/internal/handler"
	"advafaeian/go-event-broker/internal/metadata"
	"fmt"
	"log"
	"net"
)

type Server struct {
	addr     string
	metadata *metadata.MetadataLoader
}

func New(addr string, metadata *metadata.MetadataLoader) *Server {
	return &Server{
		addr:     addr,
		metadata: metadata,
	}
}

func (s *Server) Start() error {
	if err := s.metadata.Load(); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

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
		go handler.HandleConnection(conn, s.metadata)
	}
}
