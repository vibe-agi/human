package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	sdka2a "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/errordetails"
)

type principalContextKey struct{}
type listTimestampContextKey struct{}

var durableIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// PrincipalFromContext returns the identity established by NewHandler's
// authentication boundary. Domain adapters must fail closed when it is absent.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	if !ok {
		return Principal{}, false
	}
	principal.Attributes = maps.Clone(principal.Attributes)
	return principal, true
}

func withPrincipal(ctx context.Context, principal Principal) context.Context {
	principal.Attributes = maps.Clone(principal.Attributes)
	return context.WithValue(ctx, principalContextKey{}, principal)
}

type protocolHandler struct {
	next                  http.Handler
	authenticate          AuthenticateFunc
	maxRequestBytes       int64
	requiredExtensionURIs []string
	supportedExtensions   map[string]sdka2a.AgentExtension
}

func (handler *protocolHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	principal, err := handler.authenticate(request.Context(), request)
	if err != nil {
		if errors.Is(err, sdka2a.ErrUnauthorized) {
			writeProtocolError(response, http.StatusForbidden, "PERMISSION_DENIED", sdka2a.ErrUnauthorized, "permission denied")
			return
		}
		writeProtocolError(response, http.StatusUnauthorized, "UNAUTHENTICATED", sdka2a.ErrUnauthenticated, "authentication required")
		return
	}
	if err := validatePrincipal(principal); err != nil {
		writeProtocolError(response, http.StatusUnauthorized, "UNAUTHENTICATED", sdka2a.ErrUnauthenticated, "authentication required")
		return
	}
	if !supportsProtocolVersion(request) {
		protocolErr := sdka2a.NewError(sdka2a.ErrVersionNotSupported, "A2A protocol version 1.0 is required").
			WithErrorInfoMeta(map[string]string{"supportedVersions": string(sdka2a.Version)})
		writeA2AError(response, http.StatusBadRequest, "FAILED_PRECONDITION", protocolErr)
		return
	}
	if err := validateKnownQueryCardinality(request); err != nil {
		writeProtocolError(response, http.StatusBadRequest, "INVALID_ARGUMENT", sdka2a.ErrInvalidRequest, err.Error())
		return
	}

	requestedExtensions := parseExtensionHeaders(request.Header.Values(sdka2a.SvcParamExtensions))
	requestedSet := make(map[string]struct{}, len(requestedExtensions))
	for _, uri := range requestedExtensions {
		requestedSet[uri] = struct{}{}
	}
	for _, uri := range handler.requiredExtensionURIs {
		if _, requested := requestedSet[uri]; !requested {
			protocolErr := sdka2a.NewError(sdka2a.ErrExtensionSupportRequired,
				fmt.Sprintf("required A2A extension %q was not activated", uri)).
				WithErrorInfoMeta(map[string]string{"extension": uri})
			writeA2AError(response, http.StatusBadRequest, "FAILED_PRECONDITION", protocolErr)
			return
		}
	}

	if err := bufferRequestBody(request, handler.maxRequestBytes); err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			writeProtocolError(response, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED", sdka2a.ErrInvalidRequest,
				"A2A request body exceeds the configured limit")
			return
		}
		writeProtocolError(response, http.StatusBadRequest, "INVALID_ARGUMENT", sdka2a.ErrParseError, "failed to read A2A request body")
		return
	}

	// The SDK compares extension header values literally and does not split the
	// protocol's comma-separated representation. Normalize only the cloned
	// request passed inward; the authenticated callback observed the original.
	request = request.Clone(withPrincipal(request.Context(), principal))
	request.Header = request.Header.Clone()
	request.Header.Del(sdka2a.SvcParamExtensions)
	for _, uri := range requestedExtensions {
		request.Header.Add(sdka2a.SvcParamExtensions, uri)
	}
	request, err = normalizeOfficialSDKListTimestamp(request)
	if err != nil {
		writeProtocolError(response, http.StatusBadRequest, "INVALID_ARGUMENT", sdka2a.ErrInvalidRequest, err.Error())
		return
	}
	// A2A v1.0.1's normative proto HTTP annotation uses GET while its generated
	// specification and the official Go SDK use POST for SubscribeToTask. Accept
	// both published 1.0 bindings and normalize to the SDK's internal route.
	if isSubscribePath(request.URL.Path) && request.Method == http.MethodGet {
		request.Method = http.MethodPost
	}

	activated := make([]string, 0, len(requestedExtensions))
	for _, uri := range requestedExtensions {
		if _, supported := handler.supportedExtensions[uri]; supported {
			activated = append(activated, uri)
		}
	}
	response.Header().Set(sdka2a.SvcParamVersion, string(sdka2a.Version))
	if len(activated) != 0 {
		response.Header().Set(sdka2a.SvcParamExtensions, strings.Join(activated, ","))
	}
	handler.next.ServeHTTP(&protocolResponseWriter{ResponseWriter: response}, request)
}

func validatePrincipal(principal Principal) error {
	authority := string(principal.Authority)
	if !validDurableIdentity(authority) || !validIdentity(principal.Subject) {
		return errors.New("invalid authenticated principal")
	}
	return nil
}

func validDurableIdentity(value string) bool {
	return durableIdentityPattern.MatchString(value)
}

func validSQLiteTimestamp(value time.Time) bool {
	return !value.IsZero() && time.Unix(0, value.UTC().UnixNano()).UTC().Equal(value.UTC())
}

func validIdentity(value string) bool {
	if value == "" || len(value) > maxPrincipalIdentitySize || value != strings.TrimSpace(value) {
		return false
	}
	return !strings.ContainsAny(value, "\x00\r\n")
}

func supportsProtocolVersion(request *http.Request) bool {
	values := append([]string(nil), request.Header.Values(sdka2a.SvcParamVersion)...)
	values = append(values, request.URL.Query()[sdka2a.SvcParamVersion]...)
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) != string(sdka2a.Version) {
			return false
		}
	}
	return true
}

func parseExtensionHeaders(values []string) []string {
	unique := make(map[string]struct{})
	result := make([]string, 0, len(values))
	for _, value := range values {
		for _, item := range strings.Split(value, ",") {
			uri := strings.TrimSpace(item)
			if uri == "" {
				continue
			}
			if _, exists := unique[uri]; exists {
				continue
			}
			unique[uri] = struct{}{}
			result = append(result, uri)
		}
	}
	return result
}

func isSubscribePath(path string) bool {
	const prefix = "/tasks/"
	const suffix = ":subscribe"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	return id != "" && !strings.Contains(id, "/")
}

func normalizeOfficialSDKListTimestamp(request *http.Request) (*http.Request, error) {
	if request.Method != http.MethodGet || strings.TrimSuffix(request.URL.Path, "/") != "/tasks" {
		return request, nil
	}
	query := request.URL.Query()
	canonicalValues, canonical := query["statusTimestampAfter"]
	sdkValues, sdkSpelling := query["lastUpdatedAfter"]
	if canonical && sdkSpelling {
		return request, errors.New("statusTimestampAfter was supplied more than once")
	}
	values := canonicalValues
	if !canonical {
		values = sdkValues
	}
	if !canonical && !sdkSpelling {
		return request, nil
	}
	if len(values) != 1 || values[0] == "" {
		return request, errors.New("statusTimestampAfter must contain one timestamp")
	}
	parsed, err := time.Parse(time.RFC3339, values[0])
	if err != nil {
		return request, errors.New("statusTimestampAfter is invalid")
	}
	if sdkSpelling {
		// a2a-go v2.3.1's REST client emits lastUpdatedAfter while its REST server
		// reads the A2A 1.0 name statusTimestampAfter. Normalize the official client
		// spelling on the cloned request so the authenticated original is untouched.
		query["statusTimestampAfter"] = append([]string(nil), values...)
		query.Del("lastUpdatedAfter")
		request.URL.RawQuery = query.Encode()
	}
	ctx := context.WithValue(request.Context(), listTimestampContextKey{}, parsed.UTC())
	return request.WithContext(ctx), nil
}

func listTimestampFromContext(ctx context.Context) (time.Time, bool) {
	value, ok := ctx.Value(listTimestampContextKey{}).(time.Time)
	return value, ok
}

func validateKnownQueryCardinality(request *http.Request) error {
	query := request.URL.Query()
	if len(query[sdka2a.SvcParamVersion]) > 1 {
		return errors.New("A2A-Version query parameter must appear at most once")
	}
	if !strings.HasPrefix(request.URL.Path, "/tasks") {
		return nil
	}
	for _, name := range []string{
		"contextId", "status", "pageSize", "pageToken", "historyLength",
		"statusTimestampAfter", "lastUpdatedAfter", "includeArtifacts",
	} {
		if len(query[name]) > 1 {
			return fmt.Errorf("%s query parameter must appear at most once", name)
		}
	}
	return nil
}

var errRequestBodyTooLarge = errors.New("A2A request body too large")

func bufferRequestBody(request *http.Request, limit int64) error {
	if request.Body == nil || request.Body == http.NoBody || request.Method == http.MethodGet || request.Method == http.MethodHead {
		return nil
	}
	if request.ContentLength > limit {
		return errRequestBodyTooLarge
	}
	content, err := io.ReadAll(io.LimitReader(request.Body, limit+1))
	closeErr := request.Body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if int64(len(content)) > limit {
		return errRequestBodyTooLarge
	}
	request.Body = io.NopCloser(bytes.NewReader(content))
	request.ContentLength = int64(len(content))
	if len(content) != 0 {
		if err := validateJSONWire(content, requestBodyRequiresObject(request)); err != nil {
			return err
		}
	}
	return nil
}

func requestBodyRequiresObject(request *http.Request) bool {
	return request.Method == http.MethodPost &&
		(request.URL.Path == "/message:send" || request.URL.Path == "/message:stream" ||
			strings.HasSuffix(request.URL.Path, ":recordApplyReceipt"))
}

func validateJSONWire(content []byte, requireObject bool) error {
	if !utf8.Valid(content) {
		return errors.New("A2A JSON body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode A2A JSON body: %w", err)
	}
	if requireObject && first != json.Delim('{') {
		return errors.New("A2A request body must be one JSON object")
	}
	if err := validateJSONToken(decoder, first); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("A2A request body contains more than one JSON value")
		}
		return fmt.Errorf("decode trailing A2A JSON data: %w", err)
	}
	return nil
}

func validateJSONToken(decoder *json.Decoder, token json.Token) error {
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode A2A JSON object key: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("A2A JSON object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("A2A JSON object repeats field %q", key)
			}
			seen[key] = struct{}{}
			value, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode A2A JSON field %q: %w", key, err)
			}
			if err := validateJSONToken(decoder, value); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("A2A JSON object is not closed")
		}
		return nil
	case '[':
		for decoder.More() {
			value, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode A2A JSON array item: %w", err)
			}
			if err := validateJSONToken(decoder, value); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("A2A JSON array is not closed")
		}
		return nil
	default:
		return errors.New("A2A JSON contains an unexpected closing delimiter")
	}
}

// principalInterceptor exposes the already-authenticated identity through the
// SDK call context as well as the Human context value. It never authenticates
// from service parameters or payload fields.
type principalInterceptor struct {
	a2asrv.PassthroughCallInterceptor
	supportedExtensions map[string]sdka2a.AgentExtension
}

func (interceptor principalInterceptor) Before(
	ctx context.Context,
	call *a2asrv.CallContext,
	_ *a2asrv.Request,
) (context.Context, any, error) {
	principal, ok := PrincipalFromContext(ctx)
	if !ok {
		return ctx, nil, sdka2a.ErrUnauthenticated
	}
	call.User = a2asrv.NewAuthenticatedUser(principal.Subject, maps.Clone(principal.Attributes))
	if extensions, present := a2asrv.ExtensionsFrom(ctx); present {
		for _, extension := range interceptor.supportedExtensions {
			declared := extension
			if extensions.Requested(&declared) {
				extensions.Activate(&declared)
			}
		}
	}
	return ctx, nil, nil
}

type protocolResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (writer *protocolResponseWriter) WriteHeader(status int) {
	if writer.wroteHeader {
		return
	}
	writer.wroteHeader = true
	if status >= 200 && status < 300 {
		mediaType := writer.Header().Get("Content-Type")
		if strings.HasPrefix(mediaType, "application/json") {
			writer.Header().Set("Content-Type", "application/a2a+json")
		}
	}
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *protocolResponseWriter) Write(content []byte) (int, error) {
	if !writer.wroteHeader {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(content)
}

func (writer *protocolResponseWriter) Flush() {
	if !writer.wroteHeader {
		writer.WriteHeader(http.StatusOK)
	}
	if flusher, ok := writer.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (writer *protocolResponseWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

type protocolErrorEnvelope struct {
	Error protocolStatus `json:"error"`
}

type protocolStatus struct {
	Code    int                   `json:"code"`
	Status  string                `json:"status"`
	Message string                `json:"message"`
	Details []*errordetails.Typed `json:"details,omitempty"`
}

func writeProtocolError(response http.ResponseWriter, status int, grpcStatus string, base error, message string) {
	writeA2AError(response, status, grpcStatus, sdka2a.NewError(base, message))
}

func writeA2AError(response http.ResponseWriter, status int, grpcStatus string, protocolErr *sdka2a.Error) {
	if protocolErr == nil {
		protocolErr = sdka2a.NewError(sdka2a.ErrInternalError, "internal error")
	}
	protocolErr.ErrorInfo()
	details := slices.Clone(protocolErr.TypedDetails)
	// a2a-go v2.3.1's REST client parses typed google.rpc.Status errors only
	// under application/json. A2A 1.0 recommends, but does not require, the
	// vendor media type, so errors keep the official SDK's interoperable type.
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set(sdka2a.SvcParamVersion, string(sdka2a.Version))
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(protocolErrorEnvelope{Error: protocolStatus{
		Code: status, Status: grpcStatus, Message: protocolErr.Error(), Details: details,
	}})
}
