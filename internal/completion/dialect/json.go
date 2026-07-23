package dialect

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// DecodeJSON decodes exactly one JSON value while retaining integer precision
// in interface-backed fields. Dialects must not round tool arguments or tool
// results before the canonical request digest is bound.
func DecodeJSON(encoded []byte, destination any) error {
	return decodeJSON(encoded, destination, false)
}

// DecodeJSONStrict decodes exactly one JSON value, retains integer precision,
// and rejects fields which are not represented by destination. Wire dialects
// use this at their top-level envelope so a newly introduced provider control
// cannot silently become an accidental no-op.
func DecodeJSONStrict(encoded []byte, destination any) error {
	return decodeJSON(encoded, destination, true)
}

func decodeJSON(encoded []byte, destination any, strict bool) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values are not allowed")
	}
	return nil
}
