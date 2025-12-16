package handler

import (
	"advafaeian/go-event-broker/internal/protocol"
	"io"
	"log"
	"net"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var sizeBuf = make([]byte, 4)
		_, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			log.Printf("Error reading message size from connection: %v", err)
			return
		}

		reqBuf := make([]byte, protocol.BytesToInt32(sizeBuf))
		_, err = io.ReadFull(conn, reqBuf)
		if err != nil {
			log.Printf("Error reading from connection: %v", err)
			return
		}

		red := protocol.NewReader(reqBuf)

		requestHeader := protocol.RequestHeader{}

		if err := requestHeader.Decode(red); err != nil {
			log.Printf("Error parsing the request: %v", err)
			return
		}

		response := protocol.ApiVersionsResponse{
			ResponseHeader: protocol.ResponseHeader{CorrelationID: requestHeader.CorrelationId},
		}

		if err := requestHeader.Validate(); err != nil {
			pErrCode := err.(*protocol.ProtocolError).Code
			log.Printf("Error validating response: %v", err)
			response.ErrorCode = pErrCode
		} else {
			response.ErrorCode = protocol.NoError
			response.ApiKeys = protocol.SupportedApiKeys
		}

		_, err = conn.Write(response.Encode())
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}
	}
}
