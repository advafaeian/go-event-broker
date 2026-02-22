package handler

import (
	"advafaeian/go-event-broker/internal/metadata"
	"advafaeian/go-event-broker/internal/protocol"
	"slices"
	"strings"

	"io"
	"log"
	"net"
)

func HandleConnection(conn net.Conn, metadata *metadata.MetadataLoader) {

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

		red := protocol.NewReaderFromBytes(reqBuf)

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
		case protocol.FetchKey:
			response := protocol.FetchResponse{
				Header:    ResponseHeader,
				ErrorCode: pErrCode,
			}

			response.Encode(w)

		case protocol.ApiVersionsKey:

			response := protocol.ApiVersionsResponse{
				Header:    ResponseHeader,
				ErrorCode: pErrCode,
				ApiKeys:   protocol.SupportedApiKeys,
			}

			response.Encode(w)

		case protocol.DescribeTopicPartitionsKey:

			req := protocol.DescribeTopicPartitionsRequest{}

			err := req.Decode(red)
			if err != nil {
				log.Printf("Error decoding the request header: %v", err)
			}

			response := protocol.DescribeTopicPartitionsResponse{
				Header:     ResponseHeader,
				NextCursor: nil,
			}
			slices.SortFunc(req.Topics, func(a, b protocol.Topic) int {
				return strings.Compare(a.TopicName, b.TopicName)
			})

			for _, t := range req.Topics {

				topicData, err := metadata.Get(t.TopicName)

				var topic protocol.Topic

				if err != nil {
					topic = protocol.Topic{
						ErrorCode:  3,
						TopicName:  t.TopicName,
						TopicID:    protocol.UUID(make([]byte, 16)),
						IsInternal: false,
						Partitions: []protocol.Partition{},
					}
				} else {
					topic = protocol.Topic{
						ErrorCode:  0,
						TopicName:  topicData.TopicName,
						TopicID:    topicData.TopicID,
						IsInternal: false,
						Partitions: topicData.Partitions,
					}
				}
				response.Topics = append(response.Topics, topic)
			}
			response.Encode(w)
		}
		_, err = conn.Write(w.Bytes())
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}
	}
}
