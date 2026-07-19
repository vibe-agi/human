package callerhttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/llm"
)

func (running *runtime) serveHTTP(response http.ResponseWriter, request *http.Request) {
	if request == nil || request.URL == nil {
		writeAdapterError(response, http.StatusBadRequest, "invalid HTTP request")
		return
	}
	if !running.beginHandler() {
		writeAdapterError(response, http.StatusServiceUnavailable, "caller transport is shutting down")
		return
	}
	defer running.handlers.Done()
	requestBody := request.Body
	if requestBody == nil {
		requestBody = http.NoBody
	}
	var closeRequestBody sync.Once
	closeBody := func() {
		closeRequestBody.Do(func() { _ = requestBody.Close() })
	}
	defer closeBody()

	handlerCtx, cancelHandler := context.WithCancelCause(request.Context())
	controller := http.NewResponseController(response)
	lifecycleCallbackDone := make(chan struct{})
	stopLifecycleCancel := context.AfterFunc(running.lifecycle, func() {
		defer close(lifecycleCallbackDone)
		// net/http request-body reads and socket writes are not required to wake
		// merely because a derived Context was canceled. Move both connection
		// deadlines to now so Shutdown can actually join a half-open handler while
		// still leaving listener ownership with the host.
		_ = controller.SetReadDeadline(time.Now())
		_ = controller.SetWriteDeadline(time.Now())
		// Request.Body must allow Close concurrently with Read and unblock that
		// Read. This remains effective when legacy middleware hides
		// ResponseController deadline methods.
		closeBody()
		cancelHandler(context.Cause(running.lifecycle))
	})
	defer func() {
		if !stopLifecycleCancel() {
			// If shutdown won the race, wait until its deadline/cancellation
			// callback has stopped touching this handler's ResponseWriter.
			<-lifecycleCallbackDone
		}
		cancelHandler(nil)
	}()
	request = request.WithContext(handlerCtx)

	route, exists := running.config.routes[routeKey{method: request.Method, path: request.URL.Path}]
	if !exists {
		if methods := running.config.methods[request.URL.Path]; len(methods) != 0 {
			response.Header().Set("Allow", strings.Join(methods, ", "))
			writeAdapterError(response, http.StatusMethodNotAllowed, "HTTP method is not configured for this route")
			return
		}
		writeAdapterError(response, http.StatusNotFound, "HTTP route is not configured")
		return
	}

	body, err := readRequestBody(
		response,
		controller,
		requestBody,
		running.config.maxBodyBytes,
		running.config.readTimeout,
	)
	if err != nil {
		if handlerCtx.Err() != nil {
			return
		}
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeAdapterError(response, http.StatusRequestEntityTooLarge, "request body exceeds the configured limit")
			return
		}
		writeAdapterError(response, http.StatusBadRequest, "request body could not be read")
		return
	}
	if handlerCtx.Err() != nil {
		return
	}
	// Authentication receives an independent request snapshot. A custom
	// Authenticator may verify a body signature, but consuming or mutating its
	// request cannot change the bytes later decoded and durably identified by the
	// HumanLLM core.
	authRequest := request.Clone(handlerCtx)
	authRequest.Header = request.Header.Clone()
	authRequest.Body = io.NopCloser(bytes.NewReader(body))
	authRequest.ContentLength = int64(len(body))
	identity, err := running.config.authenticator.AuthenticateCaller(handlerCtx, authRequest)
	_ = authRequest.Body.Close()
	if err != nil || !stableIdentity.MatchString(string(identity.CallerID)) {
		writeAdapterError(response, http.StatusUnauthorized, "caller authentication failed")
		return
	}
	resolution, err := running.config.resolver.ResolveRequest(handlerCtx, ResolutionRequest{
		CallerID: identity.CallerID,
		Route:    route,
		Header:   request.Header.Clone(),
		Body:     bytes.Clone(body),
	})
	if err != nil {
		if handlerCtx.Err() != nil {
			return
		}
		writeAdapterError(response, http.StatusBadRequest, "caller request identity could not be resolved")
		return
	}
	if !stableIdentity.MatchString(string(resolution.IdempotencyKey)) {
		writeAdapterError(response, http.StatusBadRequest, "caller request identity could not be resolved")
		return
	}
	// This header is known before durable admission but setting it does not commit
	// an HTTP response. The adapter writes no status or body before the core has
	// returned either a safe AdmissionError or a committed ResponseDecision.
	response.Header().Set(HeaderIdempotencyKey, string(resolution.IdempotencyKey))
	admission, err := running.endpoint.Admit(handlerCtx, llm.AdmissionRequest{
		CallerID: identity.CallerID, IdempotencyKey: resolution.IdempotencyKey,
		CodecID: route.CodecID, Body: body, Task: resolution.Task,
	})
	if err != nil {
		if handlerCtx.Err() != nil {
			return
		}
		if projectAdmissionError(
			response,
			err,
			running.config.maxAdmissionErrorBodyBytes,
			running.config.writeTimeout,
		) {
			return
		}
		writeAdapterError(response, http.StatusInternalServerError, "HumanLLM admission failed")
		return
	}
	if err := validateAdmission(
		admission,
		identity.CallerID,
		resolution.IdempotencyKey,
		running.config.pageLimit,
		running.config.pageMaxBytes,
	); err != nil {
		writeAdapterError(response, http.StatusInternalServerError, "HumanLLM returned an invalid admission result")
		return
	}
	setIdentityHeaders(response.Header(), admission.Identity)
	running.writeResponse(handlerCtx, response, admission)
}

func readRequestBody(
	response http.ResponseWriter,
	controller *http.ResponseController,
	body io.ReadCloser,
	limit int64,
	timeout time.Duration,
) (result []byte, resultErr error) {
	deadlineSet := false
	if timeout > 0 {
		err := controller.SetReadDeadline(time.Now().Add(timeout))
		switch {
		case err == nil:
			deadlineSet = true
		case errors.Is(err, http.ErrNotSupported):
		default:
			return nil, err
		}
	}
	if deadlineSet {
		defer func() {
			if err := controller.SetReadDeadline(time.Time{}); resultErr == nil && err != nil {
				resultErr = err
				result = nil
			}
		}()
	}
	return io.ReadAll(http.MaxBytesReader(response, body, limit))
}

func (running *runtime) writeResponse(
	ctx context.Context,
	response http.ResponseWriter,
	admission llm.AdmissionResult,
) {
	page := admission.Response
	for !page.DecisionCommitted {
		if len(page.Events) != 0 || page.Complete {
			writeAdapterError(response, http.StatusInternalServerError, "HumanLLM response boundary is invalid")
			return
		}
		next, err := running.endpoint.WaitResponse(ctx, running.responseQuery(admission, page.Cursor))
		if err != nil {
			if ctx.Err() == nil {
				writeAdapterError(response, http.StatusInternalServerError, "HumanLLM response wait failed")
			}
			return
		}
		if err := validateResponsePage(
			next,
			admission,
			page.Cursor,
			running.config.pageLimit,
			running.config.pageMaxBytes,
		); err != nil {
			writeAdapterError(response, http.StatusInternalServerError, "HumanLLM returned an invalid response page")
			return
		}
		if next.Cursor == page.Cursor && !next.DecisionCommitted && !next.Complete {
			writeAdapterError(response, http.StatusInternalServerError, "HumanLLM response did not advance")
			return
		}
		page = next
	}
	if err := validateDecision(page.Decision); err != nil {
		writeAdapterError(response, http.StatusInternalServerError, "HumanLLM returned an invalid response decision")
		return
	}
	response.Header().Set("Content-Type", page.Decision.ContentType)
	response.Header().Del("Retry-After")
	if page.Decision.RetryAfter != "" {
		response.Header().Set("Retry-After", page.Decision.RetryAfter)
	}

	switch page.Mode {
	case llm.ResponseAggregate:
		if !page.Complete || len(page.Events) != 0 {
			writeAdapterError(response, http.StatusInternalServerError, "HumanLLM aggregate boundary is invalid")
			return
		}
		response.Header().Set("Content-Length", strconv.Itoa(len(page.Decision.Body)))
		response.WriteHeader(page.Decision.StatusCode)
		_ = writeWithDeadline(response, running.config.writeTimeout, page.Decision.Body)
	case llm.ResponseStream:
		if len(page.Decision.Body) != 0 {
			writeAdapterError(response, http.StatusInternalServerError, "HumanLLM stream boundary is invalid")
			return
		}
		if !supportsFlush(response) {
			writeAdapterError(response, http.StatusInternalServerError, "HTTP response writer does not support streaming")
			return
		}
		response.Header().Del("Content-Length")
		response.WriteHeader(page.Decision.StatusCode)
		writer := streamWriter{
			response: response, controller: http.NewResponseController(response),
			timeout: running.config.writeTimeout,
		}
		if err := writer.writeAndFlush(page.Events); err != nil {
			return
		}
		for !page.Complete {
			next, err := running.endpoint.WaitResponse(ctx, running.responseQuery(admission, page.Cursor))
			if err != nil {
				return
			}
			if err := validateResponsePage(
				next,
				admission,
				page.Cursor,
				running.config.pageLimit,
				running.config.pageMaxBytes,
			); err != nil {
				return
			}
			if next.Mode != llm.ResponseStream || !sameDecision(next.Decision, page.Decision) {
				return
			}
			if next.Cursor == page.Cursor && !next.Complete {
				return
			}
			page = next
			if err := writer.writeAndFlush(page.Events); err != nil {
				return
			}
		}
	default:
		writeAdapterError(response, http.StatusInternalServerError, "HumanLLM response mode is invalid")
	}
}

func supportsFlush(response http.ResponseWriter) bool {
	for depth := 0; response != nil && depth < 32; depth++ {
		if _, ok := response.(http.Flusher); ok {
			return true
		}
		if _, ok := response.(interface{ FlushError() error }); ok {
			return true
		}
		unwrapper, ok := response.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		response = unwrapper.Unwrap()
	}
	return false
}

func (running *runtime) responseQuery(admission llm.AdmissionResult, after uint64) llm.ResponseQuery {
	return llm.ResponseQuery{
		CallerID: admission.Identity.CallerID, IdempotencyKey: admission.Identity.IdempotencyKey,
		RequestDigest: admission.RequestDigest, After: after,
		Limit: running.config.pageLimit, MaxBytes: running.config.pageMaxBytes,
	}
}

type streamWriter struct {
	response   http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
}

func (writer streamWriter) writeAndFlush(events []llm.WireEvent) (resultErr error) {
	deadlineSet := false
	if writer.timeout > 0 {
		err := writer.controller.SetWriteDeadline(time.Now().Add(writer.timeout))
		switch {
		case err == nil:
			deadlineSet = true
		case errors.Is(err, http.ErrNotSupported):
		default:
			return err
		}
	}
	if deadlineSet {
		defer func() {
			if err := writer.controller.SetWriteDeadline(time.Time{}); resultErr == nil && err != nil {
				resultErr = err
			}
		}()
	}
	for _, event := range events {
		if _, err := writeExact(writer.response, event.Data); err != nil {
			return err
		}
	}
	return writer.controller.Flush()
}

func writeExact(writer io.Writer, data []byte) (int, error) {
	written, err := writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return written, err
}

func writeWithDeadline(
	response http.ResponseWriter,
	timeout time.Duration,
	data []byte,
) (resultErr error) {
	controller := http.NewResponseController(response)
	deadlineSet := false
	if timeout > 0 {
		err := controller.SetWriteDeadline(time.Now().Add(timeout))
		switch {
		case err == nil:
			deadlineSet = true
		case errors.Is(err, http.ErrNotSupported):
		default:
			return err
		}
	}
	if deadlineSet {
		defer func() {
			if err := controller.SetWriteDeadline(time.Time{}); resultErr == nil && err != nil {
				resultErr = err
			}
		}()
	}
	_, resultErr = writeExact(response, data)
	return resultErr
}

func validateAdmission(
	admission llm.AdmissionResult,
	caller llm.CallerID,
	idempotency llm.IdempotencyKey,
	pageLimit int,
	pageMaxBytes int64,
) error {
	if err := admission.Identity.Validate(); err != nil {
		return err
	}
	if admission.Identity.CallerID != caller || admission.Identity.IdempotencyKey != idempotency ||
		admission.RequestDigest == "" || admission.Response.RequestDigest != admission.RequestDigest ||
		admission.Response.Identity != admission.Identity {
		return errors.New("admission identity mismatch")
	}
	return validateResponsePage(admission.Response, admission, 0, pageLimit, pageMaxBytes)
}

func validateResponsePage(
	page llm.ResponsePage,
	admission llm.AdmissionResult,
	after uint64,
	pageLimit int,
	pageMaxBytes int64,
) error {
	if pageLimit == 0 {
		pageLimit = MaxResponsePageLimit
	}
	if pageMaxBytes == 0 {
		pageMaxBytes = MaxResponsePageBytes
	}
	if page.Identity != admission.Identity || page.RequestDigest != admission.RequestDigest || page.Cursor < after {
		return errors.New("response identity or cursor mismatch")
	}
	if page.Mode != admission.Response.Mode {
		return errors.New("response mode changed")
	}
	if len(page.Events) > pageLimit {
		return errors.New("response page exceeds its event limit")
	}
	pageBytes := int64(len(page.Decision.Body))
	if pageBytes > pageMaxBytes {
		return errors.New("response page exceeds its byte limit")
	}
	last := after
	for _, event := range page.Events {
		if event.Sequence <= last || event.Sequence > page.Cursor {
			return errors.New("response event ordering is invalid")
		}
		if int64(len(event.Data)) > pageMaxBytes-pageBytes {
			return errors.New("response page exceeds its byte limit")
		}
		pageBytes += int64(len(event.Data))
		last = event.Sequence
	}
	if page.DecisionCommitted {
		if err := validateDecision(page.Decision); err != nil {
			return err
		}
	} else if page.Decision.StatusCode != 0 || page.Decision.ContentType != "" ||
		page.Decision.RetryAfter != "" || len(page.Decision.Body) != 0 {
		return errors.New("uncommitted response carries a decision")
	}
	if page.Mode != llm.ResponseStream && page.Mode != llm.ResponseAggregate {
		return errors.New("response mode is invalid")
	}
	return nil
}

func validateDecision(decision llm.ResponseDecision) error {
	if decision.StatusCode < 200 || decision.StatusCode > 599 ||
		decision.StatusCode >= 300 && decision.StatusCode < 400 ||
		decision.StatusCode == http.StatusNoContent || decision.StatusCode == http.StatusResetContent ||
		!safeHeaderValue(decision.ContentType, 256) ||
		(decision.RetryAfter != "" && !safeHeaderValue(decision.RetryAfter, 128)) {
		return errors.New("response decision is invalid")
	}
	return nil
}

func sameDecision(left, right llm.ResponseDecision) bool {
	return left.StatusCode == right.StatusCode && left.ContentType == right.ContentType &&
		left.RetryAfter == right.RetryAfter && slices.Equal(left.Body, right.Body)
}

func setIdentityHeaders(header http.Header, identity llm.CompletionIdentity) {
	header.Set(HeaderIdempotencyKey, string(identity.IdempotencyKey))
	header.Set(HeaderTaskID, string(identity.TaskID))
	header.Set(HeaderRequestID, identity.RequestID)
	header.Del(HeaderWorkspaceKey)
	if identity.WorkspaceKey != "" {
		header.Set(HeaderWorkspaceKey, identity.WorkspaceKey)
	}
}

func projectAdmissionError(
	response http.ResponseWriter,
	err error,
	maxBytes int64,
	writeTimeout time.Duration,
) bool {
	var admission *llm.AdmissionError
	if !errors.As(err, &admission) {
		return false
	}
	if admission == nil || admission.Failure.Validate() != nil ||
		admission.Failure.Status < 400 || admission.Failure.Status > 599 ||
		!safeHeaderValue(admission.ContentType, 256) ||
		(admission.RetryAfter != "" && !safeHeaderValue(admission.RetryAfter, 128)) ||
		int64(len(admission.Body)) > maxBytes {
		return false
	}
	response.Header().Set("Content-Type", admission.ContentType)
	response.Header().Del("Retry-After")
	if admission.RetryAfter != "" {
		response.Header().Set("Retry-After", admission.RetryAfter)
	}
	response.Header().Set("Content-Length", strconv.Itoa(len(admission.Body)))
	response.WriteHeader(admission.Failure.Status)
	_ = writeWithDeadline(response, writeTimeout, admission.Body)
	return true
}

func safeHeaderValue(value string, maximum int) bool {
	if value == "" || len(value) > maximum || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func writeAdapterError(response http.ResponseWriter, status int, message string) {
	body := message + "\n"
	response.Header().Set("Content-Type", "text/plain; charset=utf-8")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("Content-Length", strconv.Itoa(len(body)))
	response.Header().Del("Retry-After")
	response.WriteHeader(status)
	_, _ = io.WriteString(response, body)
}
