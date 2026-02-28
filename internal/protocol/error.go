package protocol

const NoError = int16(0)
const UnknownTopicID = int16(100)
const UnknownTopicOrPartition = int16(3)
const UnsupportedVersion = int16(35)

type ProtocolError struct {
	Code    int16
	Message string
}

func (pe *ProtocolError) Error() string {
	return pe.Message
}

type VersionRange struct {
	Min int16
	Max int16
}

var SupportedVersions = func() map[int16]VersionRange {
	m := make(map[int16]VersionRange)
	for _, k := range SupportedApiKeys {
		m[k.ApiKey] = VersionRange{Min: k.MinVersion, Max: k.MaxVersion}
	}
	return m
}()

var ErrUnsupportedVersion = &ProtocolError{
	Code:    UnsupportedVersion,
	Message: "only api versions 0-4 are accepted",
}
