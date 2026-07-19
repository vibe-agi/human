// Package callerhttp provides the official HTTP caller transport for HumanLLM.
//
// The package owns neither an HTTP listener nor the HumanLLM correctness core.
// A host mounts Transport as an http.Handler, starts it with a borrowed
// llm.CallerEndpoint, and shuts down only the returned runtime. Middleware which
// wraps http.ResponseWriter must either preserve ResponseController deadline
// methods or implement Unwrap() http.ResponseWriter. An opaque wrapper can make
// a write already blocked inside that wrapper impossible to interrupt without
// also owning and closing the host's listener; in that case Shutdown returns its
// context error instead of claiming that the handler drained.
package callerhttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

const (
	// MaxRequestBodyBytes is the hard request ceiling accepted by the official
	// HTTP adapter. A host may configure a lower bound per Transport.
	MaxRequestBodyBytes int64 = 64 << 20
	// MaxAdmissionErrorBytes bounds a broken custom endpoint independently of
	// the incoming request limit. The HumanLLM core applies its Codec's smaller
	// negotiated admission-error limit first.
	MaxAdmissionErrorBytes     int64 = 128 << 20
	MaxResponsePageLimit             = 4096
	MaxResponsePageBytes       int64 = 128 << 20
	defaultBodyLimit                 = int64(8 << 20)
	defaultAdmissionErrorLimit       = MaxAdmissionErrorBytes

	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderTaskID         = "X-Human-Task-Id"
	HeaderRequestID      = "X-Human-Request-Id"
	HeaderWorkspaceKey   = "X-Human-Workspace-Key"
)

var (
	ErrNotStarted           = errors.New("HumanLLM caller HTTP transport is not started")
	ErrTransportClosed      = errors.New("HumanLLM caller HTTP transport is closed")
	ErrAlreadyStarted       = errors.New("HumanLLM caller HTTP transport is already started")
	ErrAuthentication       = errors.New("HumanLLM caller HTTP authentication failed")
	ErrResolution           = errors.New("HumanLLM caller HTTP request resolution failed")
	ErrInvalidConfiguration = errors.New("invalid HumanLLM caller HTTP configuration")
)

var stableIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var methodToken = regexp.MustCompile("^[A-Z][A-Z0-9!#$%&'*+.^_`|~-]*$")
var codecIDPattern = regexp.MustCompile(`^[a-z][a-z0-9._/-]{0,127}$`)

// Identity is the authenticated caller identity. It is the sole source of the
// llm.CallerID passed to the core; headers and RequestResolver cannot override
// it.
//
// Attributes carries advisory claims of the authenticated principal (for
// example JWT claims, mTLS SANs, or upstream-context roles) for the core's
// AdmissionPolicy and WorkerRouter. Attributes never participate in request
// identity, digests, or persistence and must not be treated as a second
// identity channel: authorization decisions bind to CallerID.
type Identity struct {
	CallerID   llm.CallerID
	Attributes map[string]any
}

// Authenticator lets an embedding host bind its own JWT, cookie, mTLS, or
// upstream-context identity system to HumanLLM. It is borrowed until the
// transport runtime reaches Done, called concurrently, and must honor context
// cancellation. The request is borrowed for the call and must not be retained
// or mutated. Return a framework Fault with CodeUnauthenticated/CodeForbidden
// and RetryNever only for a proved terminal credential decision. Return
// CodeUnavailable with RetryBackoff for a temporary identity-provider failure;
// an unclassified error is fail-closed as HTTP 503 rather than being
// misrepresented as revoked credentials.
type Authenticator interface {
	AuthenticateCaller(context.Context, *http.Request) (Identity, error)
}

type AuthenticateFunc func(context.Context, *http.Request) (Identity, error)

func (function AuthenticateFunc) AuthenticateCaller(
	ctx context.Context,
	request *http.Request,
) (Identity, error) {
	return function(ctx, request)
}

// Route explicitly binds one HTTP method and path to one registered Codec.
// No User-Agent, request-body, or header heuristic participates in routing.
type Route struct {
	Method  string
	Path    string
	CodecID llm.CodecID
}

// Config controls one reusable HTTP adapter provider. The host owns the HTTP
// server and listener. A nil Resolver selects HeaderResolver; a typed-nil
// Resolver is rejected.
type Config struct {
	Authenticator Authenticator
	Resolver      RequestResolver
	Routes        []Route
	MaxBodyBytes  int64
	// MaxAdmissionErrorBodyBytes is separate from MaxBodyBytes: reducing the
	// caller request budget must not turn a valid Codec error into a generic 500.
	MaxAdmissionErrorBodyBytes int64
	ReadTimeout                time.Duration
	WriteTimeout               time.Duration
	// PageLimit and PageMaxBytes bound both the query sent to CallerEndpoint and
	// every page accepted back from it. Zero delegates the query default to the
	// endpoint while the adapter still enforces its exported hard ceiling.
	PageLimit    int
	PageMaxBytes int64
}

type routeKey struct {
	method string
	path   string
}

type resolvedConfig struct {
	authenticator              Authenticator
	resolver                   RequestResolver
	routes                     map[routeKey]Route
	methods                    map[string][]string
	maxBodyBytes               int64
	maxAdmissionErrorBodyBytes int64
	readTimeout                time.Duration
	writeTimeout               time.Duration
	pageLimit                  int
	pageMaxBytes               int64
}

func (config Config) resolve() (resolvedConfig, error) {
	if nilInterface(config.Authenticator) {
		return resolvedConfig{}, fmt.Errorf("%w: authenticator is required", ErrInvalidConfiguration)
	}
	resolver := config.Resolver
	if resolver == nil {
		resolver = HeaderResolver{}
	} else if nilInterface(resolver) {
		return resolvedConfig{}, fmt.Errorf("%w: resolver is typed nil", ErrInvalidConfiguration)
	}
	if len(config.Routes) == 0 {
		return resolvedConfig{}, fmt.Errorf("%w: at least one route is required", ErrInvalidConfiguration)
	}
	routes := make(map[routeKey]Route, len(config.Routes))
	methods := make(map[string][]string)
	for _, candidate := range config.Routes {
		route, err := validateRoute(candidate)
		if err != nil {
			return resolvedConfig{}, fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
		}
		key := routeKey{method: route.Method, path: route.Path}
		if _, duplicate := routes[key]; duplicate {
			return resolvedConfig{}, fmt.Errorf(
				"%w: duplicate route %s %s", ErrInvalidConfiguration, route.Method, route.Path,
			)
		}
		routes[key] = route
		methods[route.Path] = append(methods[route.Path], route.Method)
	}
	for path := range methods {
		methods[path] = sortedUnique(methods[path])
	}
	maxBodyBytes := config.MaxBodyBytes
	if maxBodyBytes == 0 {
		maxBodyBytes = defaultBodyLimit
	}
	if maxBodyBytes < 1 || maxBodyBytes > MaxRequestBodyBytes {
		return resolvedConfig{}, fmt.Errorf(
			"%w: body limit must be 1..%d", ErrInvalidConfiguration, MaxRequestBodyBytes,
		)
	}
	maxAdmissionErrorBodyBytes := config.MaxAdmissionErrorBodyBytes
	if maxAdmissionErrorBodyBytes == 0 {
		maxAdmissionErrorBodyBytes = defaultAdmissionErrorLimit
	}
	if maxAdmissionErrorBodyBytes < 1 || maxAdmissionErrorBodyBytes > MaxAdmissionErrorBytes {
		return resolvedConfig{}, fmt.Errorf(
			"%w: admission error body limit must be 1..%d",
			ErrInvalidConfiguration,
			MaxAdmissionErrorBytes,
		)
	}
	writeTimeout := config.WriteTimeout
	readTimeout := config.ReadTimeout
	if readTimeout == 0 {
		readTimeout = 30 * time.Second
	}
	if writeTimeout == 0 {
		writeTimeout = 10 * time.Second
	}
	if readTimeout <= 0 || writeTimeout <= 0 {
		return resolvedConfig{}, fmt.Errorf("%w: read and write timeouts must be positive", ErrInvalidConfiguration)
	}
	if config.PageLimit < 0 || config.PageLimit > MaxResponsePageLimit ||
		config.PageMaxBytes < 0 || config.PageMaxBytes > MaxResponsePageBytes {
		return resolvedConfig{}, fmt.Errorf(
			"%w: response PageLimit must be 0..%d and PageMaxBytes must be 0..%d",
			ErrInvalidConfiguration,
			MaxResponsePageLimit,
			MaxResponsePageBytes,
		)
	}
	return resolvedConfig{
		authenticator:              config.Authenticator,
		resolver:                   resolver,
		routes:                     routes,
		methods:                    methods,
		maxBodyBytes:               maxBodyBytes,
		maxAdmissionErrorBodyBytes: maxAdmissionErrorBodyBytes,
		readTimeout:                readTimeout,
		writeTimeout:               writeTimeout,
		pageLimit:                  config.PageLimit,
		pageMaxBytes:               config.PageMaxBytes,
	}, nil
}

func validateRoute(route Route) (Route, error) {
	if !methodToken.MatchString(route.Method) || route.Method == http.MethodHead {
		return Route{}, fmt.Errorf("route method %q is invalid or cannot carry an exact response body", route.Method)
	}
	if !validRoutePath(route.Path) {
		return Route{}, fmt.Errorf("route path %q is invalid", route.Path)
	}
	if !codecIDPattern.MatchString(string(route.CodecID)) {
		return Route{}, fmt.Errorf("route Codec ID is invalid")
	}
	return route, nil
}

func validRoutePath(value string) bool {
	if value == "" || len(value) > 2048 || !utf8.ValidString(value) || value[0] != '/' ||
		strings.ContainsAny(value, "?#") {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

// Transport implements both llm.CallerTransport and http.Handler. A Transport
// may be started once; construct a fresh one for a later composition lifetime.
type Transport struct {
	config resolvedConfig

	mu      sync.RWMutex
	runtime *runtime
	started bool
}

var (
	_ llm.CallerTransport = (*Transport)(nil)
	_ http.Handler        = (*Transport)(nil)
)

func New(config Config) (*Transport, error) {
	resolved, err := config.resolve()
	if err != nil {
		return nil, err
	}
	return &Transport{config: resolved}, nil
}

func (*Transport) Description() llm.CallerTransportDescription {
	return llm.CallerTransportDescription{
		Contract: framework.Contract{
			ID: llm.CallerTransportContractID, Major: llm.CallerTransportContractMajor,
		},
		Provider: "http",
		Version:  "1",
	}
}

func (transport *Transport) Start(
	ctx context.Context,
	endpoint llm.CallerEndpoint,
) (llm.CallerTransportRuntime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nilInterface(endpoint) {
		return nil, fmt.Errorf("%w: endpoint is required", ErrInvalidConfiguration)
	}
	if _, err := llm.ValidateCallerTransport(transport); err != nil {
		return nil, err
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.started {
		return nil, ErrAlreadyStarted
	}
	transport.started = true
	lifecycle, cancel := context.WithCancelCause(context.Background())
	running := &runtime{
		config:    transport.config,
		endpoint:  endpoint,
		lifecycle: lifecycle,
		cancel:    cancel,
		done:      make(chan struct{}),
		drained:   make(chan struct{}),
	}
	transport.runtime = running
	return running, nil
}

func (transport *Transport) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	transport.mu.RLock()
	running := transport.runtime
	transport.mu.RUnlock()
	if running == nil {
		writeAdapterError(response, http.StatusServiceUnavailable, "caller transport is not started")
		return
	}
	running.serveHTTP(response, request)
}

type runtime struct {
	config   resolvedConfig
	endpoint llm.CallerEndpoint

	lifecycle context.Context
	cancel    context.CancelCauseFunc
	done      chan struct{}

	mu        sync.Mutex
	closing   bool
	handlers  sync.WaitGroup
	drained   chan struct{}
	drainOnce sync.Once
	finish    sync.Once
	err       error
}

var _ llm.CallerTransportRuntime = (*runtime)(nil)

func (running *runtime) Done() <-chan struct{} { return running.done }

func (running *runtime) Err() error {
	select {
	case <-running.done:
		return running.err
	default:
		return nil
	}
}

func (running *runtime) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is required", ErrInvalidConfiguration)
	}
	running.mu.Lock()
	if !running.closing {
		running.closing = true
		running.cancel(ErrTransportClosed)
	}
	running.mu.Unlock()
	running.drainOnce.Do(func() {
		go func() {
			running.handlers.Wait()
			close(running.drained)
		}()
	})
	select {
	case <-running.drained:
	case <-ctx.Done():
		return ctx.Err()
	}
	running.finish.Do(func() { close(running.done) })
	return running.err
}

func (running *runtime) beginHandler() bool {
	running.mu.Lock()
	defer running.mu.Unlock()
	if running.closing {
		return false
	}
	running.handlers.Add(1)
	return true
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
