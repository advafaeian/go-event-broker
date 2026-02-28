package handler

import (
	"advafaeian/go-event-broker/internal/metadata"
	"advafaeian/go-event-broker/internal/protocol"
)

func HandleApiVersions(w *protocol.Writer, r *protocol.Reader, metadataLoader *metadata.MetadataLoader, errCode int16) error {

	response := protocol.ApiVersionsResponse{
		ErrorCode: errCode,
		ApiKeys:   protocol.SupportedApiKeys,
	}

	response.Encode(w)
	return nil
}
