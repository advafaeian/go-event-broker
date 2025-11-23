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
	if err := protocol.ParseRequest(buf, &request); err != nil {
		log.Printf("Error parsing the request: %v", err)
		return
	}

	errorCode := int16(0)
	if err := request.Validate(); err != nil {
		pErrCode := err.(*protocol.ProtocolError).Code
		switch pErrCode {
		case protocol.ErrUnsupportedVersion.Code:
			log.Printf("Error validating response: %v", err)
			errorCode = pErrCode
		}
	}

	response := protocol.Response{
		MessageSize:   0,
		CorrelationID: request.CorrelationId,
		ErrorCode:     errorCode,
	}

	_, err = conn.Write(response.Encode())
	if err != nil {
		log.Printf("Error writing response: %v", err)
		return
	}
}
