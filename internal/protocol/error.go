package protocol

const NoError = int16(0)
const UnknownTopicID = int16(100)

type ProtocolError struct {
	Code    int16
	Message string
}

func (pe *ProtocolError) Error() string {
	return pe.Message
}

var SupportedApiKeys = []ApiKey{
	{ApiKey: ApiVersionsKey, MinVersion: 0, MaxVersion: 4},
	{ApiKey: DescribeTopicPartitionsKey, MinVersion: 0, MaxVersion: 0},
	{ApiKey: FetchKey, MinVersion: 0, MaxVersion: 16},
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
	Code:    35,
	Message: "only api versions 0-4 are accepted",
}
