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
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values are not allowed")
	}
	return nil
}
