package handler

import (
	"advafaeian/go-event-broker/internal/protocol"
	"io"
	"log"
	"net"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()

	var buf = make([]byte, 12)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Printf("Error reading from connection: %v", err)
		return
	}

	correlation_id := buf[8:12]
	response := append(protocol.IntToBytes(0), correlation_id...)

	_, err = conn.Write(response)
	if err != nil {
		log.Printf("Error writing response: %v", err)
		return
	}
}
