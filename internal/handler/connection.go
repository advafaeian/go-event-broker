package handler

import (
	"advafaeian/go-event-broker/internal/protocol"
	"io"
	"log"
	"net"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()

	var sizeBuf = make([]byte, 4)
	_, err := io.ReadFull(conn, sizeBuf)
	if err != nil {
		log.Printf("Error reading message size from connection: %v", err)
		return
	}

	var reqBuf = make([]byte, protocol.BytesToInt32(sizeBuf))
	_, err = io.ReadFull(conn, reqBuf)
	if err != nil {
		log.Printf("Error reading from connection: %v", err)
		return
	}

	request := protocol.Request{}
	if err := protocol.ParseRequest(reqBuf, &request); err != nil {
		log.Printf("Error parsing the request: %v", err)
		return
	}

	var response protocol.ApiVersionsResponse
	if err := request.Validate(); err != nil {
		pErrCode := err.(*protocol.ProtocolError).Code
		log.Printf("Error validating response: %v", err)
		response = protocol.ApiVersionsResponse{
			CorrelationID: request.CorrelationId,
			ErrorCode:     pErrCode,
		}
	} else {
		response = protocol.ApiVersionsResponse{
			CorrelationID: request.CorrelationId,
			ErrorCode:     int16(0),
			ApiKeys: []protocol.ApiKey{{ApiKey: 1, MinVersion: 0, MaxVersion: 11, TagBuffer: []protocol.TaggedField{}},
				{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TagBuffer: []protocol.TaggedField{}},
				{ApiKey: 75, MinVersion: 0, MaxVersion: 0, TagBuffer: []protocol.TaggedField{}},
			},
			ThrottleMs: 0,
			TagBuffer:  []protocol.TaggedField{},
		}
	}

	_, err = conn.Write(response.Encode())
	if err != nil {
		log.Printf("Error writing response: %v", err)
		return
	}
}
