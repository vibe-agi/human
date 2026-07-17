package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

const (
	headerCodexTurnMetadata       = "X-Codex-Turn-Metadata"
	maxCodexTurnMetadataBytes     = 16 << 10
	maxCodexJSONNestingDepth      = 256
	codexRetryKeyDomain           = "human:auto:codex-turn:v1"
	codexDerivedIdempotencyPrefix = "auto:codex-turn:v1:"
)

// resolveRequestIdempotencyKey recognizes the narrow retry identity exposed by
// Codex Responses requests. It is deliberately not a general body-hash
// deduplicator: only authenticated Chat traffic with an exact Codex turn UUID
// can acquire a derived correctness key. Callers that provide an explicit key
// always retain control of that namespace.
//
// An empty key and nil error mean this is not a recognized Codex turn and the
// caller should allocate the ordinary random request key. Once a matching
// Codex client supplies the metadata header, malformed identity material fails
// closed instead of silently creating duplicate work under random keys.
func resolveRequestIdempotencyKey(
	explicitKey string,
	callerID string,
	tier completion.CapabilityTier,
	header http.Header,
	userAgent string,
	request canonical.Request,
	rawBody []byte,
) (string, error) {
	if explicitKey != "" {
		return explicitKey, nil
	}
	if callerID == "" || tier != completion.TierChat ||
		request.Dialect != canonical.DialectResponses ||
		!strings.HasPrefix(userAgent, "codex_exec/") {
		return "", nil
	}

	values := header.Values(headerCodexTurnMetadata)
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 {
		return "", errors.New("Codex turn metadata must use exactly one header value")
	}
	if len(values[0]) == 0 || len(values[0]) > maxCodexTurnMetadataBytes {
		return "", errors.New("Codex turn metadata is empty or too large")
	}
	metadataValue, err := decodeUniqueJSON([]byte(values[0]))
	if err != nil {
		return "", errors.New("Codex turn metadata is not unambiguous JSON")
	}
	metadata, ok := metadataValue.(map[string]any)
	if !ok {
		return "", errors.New("Codex turn metadata must be a JSON object")
	}
	requestKindValue, hasRequestKind := metadata["request_kind"]
	if !hasRequestKind {
		return "", nil
	}
	requestKind, requestKindOK := requestKindValue.(string)
	if !requestKindOK {
		return "", errors.New("Codex request_kind must be a string")
	}
	if requestKind != "turn" {
		return "", nil
	}
	turnID, turnIDOK := metadata["turn_id"].(string)
	if !turnIDOK || !canonicalNonNilUUID(turnID) {
		return "", errors.New("Codex turn_id must be a canonical non-nil UUID")
	}

	canonicalDigest, err := request.Digest()
	if err != nil {
		return "", errors.New("Codex request has no canonical digest")
	}
	semanticBodyDigest, err := jsonSemanticDigest(rawBody)
	if err != nil {
		return "", errors.New("Codex request body is not unambiguous JSON")
	}

	digest := sha256.New()
	writeDigestField(digest, codexRetryKeyDomain)
	writeDigestField(digest, callerID)
	writeDigestField(digest, turnID)
	writeDigestField(digest, canonicalDigest)
	writeDigestField(digest, hex.EncodeToString(semanticBodyDigest[:]))
	return codexDerivedIdempotencyPrefix + hex.EncodeToString(digest.Sum(nil)), nil
}

func canonicalNonNilUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func writeDigestField(destination hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = destination.Write(size[:])
	_, _ = destination.Write([]byte(value))
}

func jsonSemanticDigest(payload []byte) ([sha256.Size]byte, error) {
	value, err := decodeUniqueJSON(payload)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	canonicalPayload, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonicalPayload), nil
}

// decodeUniqueJSON decodes exactly one JSON value while retaining json.Number
// precision and rejecting duplicate object keys at every nesting depth. A
// normal map unmarshal would silently let a later duplicate replace an earlier
// value, which would make a derived idempotency identity ambiguous.
func decodeUniqueJSON(payload []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	value, err := decodeUniqueJSONValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func decodeUniqueJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > maxCodexJSONNestingDepth {
		return nil, errors.New("JSON nesting is too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}

	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, errors.New("duplicate JSON object key")
			}
			value, err := decodeUniqueJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if closing != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := decodeUniqueJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if closing != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}
