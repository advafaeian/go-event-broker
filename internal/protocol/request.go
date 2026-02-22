package protocol

import "errors"

type ClientID string

type RequestHeader struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientID          string
	TagBuffer         TagBuffer
}

func (req *RequestHeader) Decode(red *Reader) error {
	var err error

	if req.RequestApiKey, err = red.Int16(); err != nil {
		return err
	}
	if req.RequestApiVersion, err = red.Int16(); err != nil {
		return err
	}
	if req.CorrelationId, err = red.Int32(); err != nil {
		return err
	}

	clientIDLength, err := red.Int16()
	if err != nil {
		return err
	}

	for i := 0; i < int(clientIDLength); i++ {
		b, err := red.Byte()
		if err != nil {
			return err
		}
		req.ClientID += string(b)
	}

	req.TagBuffer = red.TagBuffer()
	return nil
}

func (r *RequestHeader) Validate() error {

	versionRange, ok := SupportedVersions[r.RequestApiKey]

	if !ok {
		return errors.New("Api key is not valid")
	}

	if r.RequestApiVersion < versionRange.Min || r.RequestApiVersion > versionRange.Max {
		return ErrUnsupportedVersion
	}
	return nil
}
