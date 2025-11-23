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

	request := protocol.Request{}
	protocol.ParseRequest(buf, &request)

	response := protocol.Response{
		MessageSize:   0,
		CorrelationID: request.CorrelationId,
		ErrorCode:     0,
	}

	_, err = conn.Write(response.Encode())
	if err != nil {
		log.Printf("Error writing response: %v", err)
		return
	}
}
