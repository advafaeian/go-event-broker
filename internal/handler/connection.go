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
			log.Printf("Error parsing the request header: %v", err)
			return
		}

		pErrCode := protocol.NoError
		if err := requestHeader.Validate(); err != nil {
			pErrCode = err.(*protocol.ProtocolError).Code
		}

		w := protocol.NewWriter()

		ResponseHeader := protocol.ResponseHeader{CorrelationID: requestHeader.CorrelationId}

		switch requestHeader.RequestApiKey {
		case protocol.ApiVersionsKey:

			response := protocol.ApiVersionsResponse{
				Header:    ResponseHeader,
				ErrorCode: pErrCode,
				ApiKeys:   protocol.SupportedApiKeys,
			}

			response.Encode(w)

			_, err = conn.Write(w.Bytes())

		case protocol.DescribeTopicPartitionsKey:
			response := protocol.DescribeTopicPartitionsResponse{
				Header: ResponseHeader,
			}
			response.Encode(w)
			_, err = conn.Write(w.Bytes())
		}

		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}
	}
}
