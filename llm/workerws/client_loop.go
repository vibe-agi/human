package workerws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

type incomingClientMessage struct {
	message envelope
	err     error
}

func (client *Client) connectionLoop() error {
	var attempt uint32
	for {
		if err := client.ctx.Err(); err != nil {
			return nil
		}
		started := time.Now()
		err := client.runSession()
		if client.ctx.Err() != nil {
			return nil
		}
		if isPermanentClientError(err) {
			return err
		}
		if time.Since(started) >= client.config.ReconnectResetAfter {
			attempt = 0
		}
		if attempt != ^uint32(0) {
			attempt++
		}
		delay, err := client.reconnectDelay(attempt)
		if err != nil {
			return permanentClientError(err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-client.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func (client *Client) reconnectDelay(attempt uint32) (time.Duration, error) {
	if client.config.Backoff != nil {
		delay := client.config.Backoff.Delay(attempt)
		if delay <= 0 {
			return 0, fmt.Errorf("%w: reconnect backoff returned non-positive delay", ErrClientConfiguration)
		}
		if delay > client.config.ReconnectMaxDelay {
			delay = client.config.ReconnectMaxDelay
		}
		return delay, nil
	}
	delay := client.config.ReconnectMinDelay
	for remaining := attempt; remaining > 1 && delay < client.config.ReconnectMaxDelay; remaining-- {
		if delay > client.config.ReconnectMaxDelay/2 {
			return client.config.ReconnectMaxDelay, nil
		}
		delay *= 2
	}
	if delay > client.config.ReconnectMaxDelay {
		delay = client.config.ReconnectMaxDelay
	}
	return delay, nil
}

func (client *Client) runSession() error {
	sessionID, err := newWorkerSessionID()
	if err != nil {
		return permanentClientError(err)
	}
	header, err := client.dialHeader()
	if err != nil {
		if headerFailurePermanent(err) {
			return permanentClientError(err)
		}
		return err
	}
	header.Set(SessionHeader, string(sessionID))

	dialCtx, cancelDial := context.WithTimeout(client.ctx, client.config.ConnectTimeout)
	connection, response, err := websocket.Dial(dialCtx, client.config.parsedURL, &websocket.DialOptions{
		HTTPClient: client.config.HTTPClient,
		HTTPHeader: header,
	})
	cancelDial()
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return classifyDialFailure(response, err)
	}
	defer connection.CloseNow()
	connection.SetReadLimit(client.config.ReadLimit)

	principal := llm.AuthenticatedWorker{
		WorkerID:  client.config.Worker,
		SessionID: sessionID,
	}
	handshakeCtx, cancelHandshake := context.WithTimeout(client.ctx, client.config.ConnectTimeout)
	first, err := readClientEnvelope(handshakeCtx, connection)
	cancelHandshake()
	if err != nil {
		return classifyConnectionFailure(err)
	}
	if err := first.validateInbound(messageHello); err != nil {
		return permanentClientError(fmt.Errorf("%w: %v", ErrClientProtocol, err))
	}
	greeting, err := decodePayload[hello](first)
	if err != nil {
		return permanentClientError(fmt.Errorf("%w: %v", ErrClientProtocol, err))
	}
	if greeting.Gateway != string(client.config.Gateway) ||
		greeting.Worker != string(principal.WorkerID) || greeting.Session != string(principal.SessionID) {
		return permanentClientError(fmt.Errorf("%w: gateway hello does not match expected authenticated worker", ErrClientAuthentication))
	}

	incoming := make(chan incomingClientMessage, 1)
	sessionCtx, cancelSession := context.WithCancel(client.ctx)
	defer cancelSession()
	go readClientMessages(sessionCtx, connection, incoming)

	var inFlight *JournalEvent
	if err := client.flushPendingEvent(sessionCtx, connection, principal, &inFlight); err != nil {
		return err
	}
	for {
		select {
		case <-client.ctx.Done():
			return nil
		case <-client.eventWake:
			if err := client.flushPendingEvent(sessionCtx, connection, principal, &inFlight); err != nil {
				return err
			}
		case result := <-incoming:
			if result.err != nil {
				return classifyConnectionFailure(result.err)
			}
			if err := client.handleServerMessage(sessionCtx, connection, principal, &inFlight, result.message); err != nil {
				return err
			}
		}
	}
}

func (client *Client) dialHeader() (http.Header, error) {
	header := client.config.HTTPHeader.Clone()
	if header == nil {
		header = make(http.Header)
	}
	if client.config.HeaderProvider == nil {
		return header, nil
	}
	provided, err := client.config.HeaderProvider.WorkerHeaders(client.ctx)
	if err != nil {
		return nil, fmt.Errorf("provide HumanLLM worker authentication headers: %w", err)
	}
	for name, values := range provided {
		header.Del(name)
		for _, value := range values {
			header.Add(name, value)
		}
	}
	return header, nil
}

func headerFailurePermanent(err error) bool {
	code, retry, ok := framework.FaultInfo(err)
	return ok && (code == framework.CodeUnauthenticated || code == framework.CodeForbidden) && retry == framework.RetryNever
}

func classifyDialFailure(response *http.Response, err error) error {
	if response == nil {
		return err
	}
	status := response.StatusCode
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return permanentClientError(fmt.Errorf("%w: HTTP %d", ErrClientAuthentication, status))
	}
	if status >= 400 && status < 500 && status != http.StatusRequestTimeout &&
		status != http.StatusConflict && status != http.StatusTooEarly && status != http.StatusTooManyRequests {
		return permanentClientError(fmt.Errorf("%w: gateway rejected WebSocket handshake with HTTP %d", ErrClientProtocol, status))
	}
	return err
}

func classifyConnectionFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrClientProtocol) {
		return permanentClientError(err)
	}
	status := websocket.CloseStatus(err)
	if status == websocket.StatusUnsupportedData || status == websocket.StatusInvalidFramePayloadData {
		return permanentClientError(fmt.Errorf("%w: %v", ErrClientProtocol, err))
	}
	if status == websocket.StatusPolicyViolation {
		var closeFailure websocket.CloseError
		if errors.As(err, &closeFailure) {
			reason := strings.ToLower(closeFailure.Reason)
			if strings.Contains(reason, "connection conflict") {
				return permanentClientError(fmt.Errorf("%w: %v", ErrClientConnectionConflict, err))
			}
			if strings.Contains(reason, "connection closed") {
				return err
			}
		}
		return permanentClientError(fmt.Errorf("%w: %v", ErrClientProtocol, err))
	}
	return err
}

func readClientMessages(ctx context.Context, connection *websocket.Conn, incoming chan<- incomingClientMessage) {
	for {
		message, err := readClientEnvelope(ctx, connection)
		select {
		case incoming <- incomingClientMessage{message: message, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func readClientEnvelope(ctx context.Context, connection *websocket.Conn) (envelope, error) {
	kind, encoded, err := connection.Read(ctx)
	if err != nil {
		return envelope{}, err
	}
	if kind != websocket.MessageText {
		return envelope{}, fmt.Errorf("%w: HumanLLM worker protocol requires text JSON messages", ErrClientProtocol)
	}
	var message envelope
	if err := decodeStrictJSON(encoded, &message); err != nil {
		return envelope{}, fmt.Errorf("%w: decode envelope: %v", ErrClientProtocol, err)
	}
	return message, nil
}

func (client *Client) handleServerMessage(
	ctx context.Context,
	connection *websocket.Conn,
	principal llm.AuthenticatedWorker,
	inFlight **JournalEvent,
	message envelope,
) error {
	if err := message.validateInbound(messageAssignment, messageEventReceipt); err != nil {
		return permanentClientError(fmt.Errorf("%w: %v", ErrClientProtocol, err))
	}
	switch message.Type {
	case messageAssignment:
		delivery, err := decodePayload[llm.WorkerAssignmentDelivery](message)
		if err != nil || delivery.ValidateFor(principal) != nil {
			return permanentClientError(fmt.Errorf("%w: invalid assignment", ErrClientProtocol))
		}
		digest, err := digestJournalValue(delivery)
		if err != nil {
			return permanentClientError(err)
		}
		state, err := client.config.journal.PutAssignment(ctx, JournalAssignment{
			Digest: digest, Delivery: llm.CloneWorkerAssignmentDelivery(delivery),
		})
		// A commit-unknown result may already be durable. Waking this read-only
		// projector reconciles the outcome without emitting a wire ACK.
		signal(client.assignmentWake)
		if err != nil {
			wrapped := &JournalError{Operation: "put assignment", Delivery: delivery.ID, Cause: err}
			if journalFailurePermanent(err) {
				return permanentClientError(wrapped)
			}
			return wrapped
		}
		if state != JournalEntryPending && state != JournalEntrySettled {
			return permanentClientError(&JournalError{Operation: "put assignment", Delivery: delivery.ID, Cause: ErrJournalCorrupt})
		}
		// This is deliberately after PutAssignment's durability boundary.
		return client.writeClientMessage(ctx, connection, messageAssignmentACK, delivery.ID)

	case messageEventReceipt:
		receipt, err := decodePayload[llm.WorkerEventReceipt](message)
		if err != nil {
			return permanentClientError(fmt.Errorf("%w: invalid event receipt", ErrClientProtocol))
		}
		if *inFlight == nil {
			// A duplicate receipt after local settlement changes no state. Receipts
			// are per-delivery rather than cumulative, so it cannot settle another
			// outbox entry.
			return nil
		}
		record := **inFlight
		if receipt.Delivery != record.Delivery.ID {
			return permanentClientError(fmt.Errorf("%w: event receipt is not for the in-flight FIFO head", ErrClientProtocol))
		}
		if err := receipt.ValidateFor(record.Delivery); err != nil {
			return permanentClientError(fmt.Errorf("%w: %v", ErrClientProtocol, err))
		}
		receiptDigest, err := digestJournalValue(receipt)
		if err != nil {
			return permanentClientError(err)
		}
		settleErr := client.config.journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest)
		if receipt.Decision == llm.WorkerEventNACK {
			// Commit-unknown may mean the NACK inbox record is already durable.
			signal(client.rejectionWake)
		}
		if settleErr != nil {
			wrapped := &JournalError{Operation: "settle event", Delivery: receipt.Delivery, Cause: settleErr}
			if journalFailurePermanent(settleErr) {
				return permanentClientError(wrapped)
			}
			return wrapped
		}
		*inFlight = nil
		return client.flushPendingEvent(ctx, connection, principal, inFlight)
	default:
		return permanentClientError(fmt.Errorf("%w: unexpected message", ErrClientProtocol))
	}
}

func (client *Client) flushPendingEvent(
	ctx context.Context,
	connection *websocket.Conn,
	principal llm.AuthenticatedWorker,
	inFlight **JournalEvent,
) error {
	if *inFlight != nil {
		return nil
	}
	records, err := client.config.journal.ListEvents(ctx, 0, 1)
	if err != nil {
		wrapped := &JournalError{Operation: "list events", Cause: err}
		if journalFailurePermanent(err) {
			return permanentClientError(wrapped)
		}
		return wrapped
	}
	if len(records) == 0 {
		return nil
	}
	record := records[0]
	if err := validateJournalEvent(record, 0, principal); err != nil {
		return permanentClientError(err)
	}
	if err := client.validateEventWireSize(record.Delivery); err != nil {
		return permanentClientError(&JournalError{
			Operation: "validate pending event wire size", Delivery: record.Delivery.ID,
			Cause: errors.Join(ErrClientConfiguration, err),
		})
	}
	if err := client.writeClientMessage(ctx, connection, messageEvent, record.Delivery); err != nil {
		return err
	}
	copy := record
	copy.Delivery = llm.CloneWorkerEventDelivery(record.Delivery)
	*inFlight = &copy
	return nil
}

func (client *Client) writeClientMessage(ctx context.Context, connection *websocket.Conn, kind messageType, payload any) error {
	message, err := newEnvelope(kind, payload)
	if err != nil {
		return permanentClientError(fmt.Errorf("%w: encode %s: %v", ErrClientProtocol, kind, err))
	}
	writeCtx, cancel := context.WithTimeout(ctx, client.config.WriteTimeout)
	defer cancel()
	return wsjson.Write(writeCtx, connection, message)
}

func validateJournalEvent(record JournalEvent, after JournalSequence, principal llm.AuthenticatedWorker) error {
	if record.Sequence == 0 || record.Sequence <= after {
		return &JournalError{Operation: "list events", Delivery: record.Delivery.ID, Cause: ErrJournalCorrupt}
	}
	if err := record.Digest.Validate(); err != nil {
		return &JournalError{Operation: "list events", Delivery: record.Delivery.ID, Cause: err}
	}
	if err := record.Delivery.ValidateFor(principal); err != nil {
		return &JournalError{Operation: "list events", Delivery: record.Delivery.ID, Cause: errors.Join(ErrJournalCorrupt, err)}
	}
	digest, err := digestJournalValue(record.Delivery)
	if err != nil || digest != record.Digest {
		return &JournalError{Operation: "list events", Delivery: record.Delivery.ID, Cause: ErrJournalCorrupt}
	}
	return nil
}

func (client *Client) assignmentPump() {
	client.notificationPump(client.assignmentWake, client.presentAssignmentsPage)
}

func (client *Client) rejectionPump() {
	client.notificationPump(client.rejectionWake, client.presentRejectionsPage)
}

func (client *Client) notificationPump(wake <-chan struct{}, present func(context.Context) error) {
	delay := client.config.ReconnectMinDelay
	for {
		err := present(client.ctx)
		if err != nil {
			if journalFailurePermanent(err) {
				client.stopAdmission(permanentClientError(err))
				return
			}
			timer := time.NewTimer(delay)
			select {
			case <-client.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			if delay < client.config.ReconnectMaxDelay {
				delay *= 2
				if delay > client.config.ReconnectMaxDelay {
					delay = client.config.ReconnectMaxDelay
				}
			}
			continue
		}
		delay = client.config.ReconnectMinDelay
		select {
		case <-client.ctx.Done():
			return
		case <-wake:
		}
	}
}

func (client *Client) presentAssignmentsPage(ctx context.Context) error {
	principal := llm.AuthenticatedWorker{WorkerID: client.config.Worker, SessionID: "journal-presentation"}
	var after JournalSequence
	for {
		records, err := client.config.journal.ListAssignments(ctx, after, defaultJournalPageSize)
		if err != nil {
			return &JournalError{Operation: "list assignments", Cause: err}
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			if record.Sequence == 0 || record.Sequence <= after || record.Digest.Validate() != nil || record.Delivery.ValidateFor(principal) != nil {
				return &JournalError{Operation: "list assignments", Delivery: record.Delivery.ID, Cause: ErrJournalCorrupt}
			}
			digest, err := digestJournalValue(record.Delivery)
			if err != nil || digest != record.Digest {
				return &JournalError{Operation: "list assignments", Delivery: record.Delivery.ID, Cause: ErrJournalCorrupt}
			}
			after = record.Sequence
			client.presentMu.Lock()
			prior, presented := client.presentAssignments[record.Delivery.ID]
			if presented && prior != record.Digest {
				client.presentMu.Unlock()
				return &JournalError{Operation: "present assignment", Delivery: record.Delivery.ID, Cause: ErrJournalCorrupt}
			}
			if !presented {
				client.presentAssignments[record.Delivery.ID] = record.Digest
			}
			client.presentMu.Unlock()
			if presented {
				continue
			}
			select {
			case client.assignments <- llm.CloneWorkerAssignmentDelivery(record.Delivery):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// Journal promises at most limit, not a full page. Continue from the
		// validated cursor until an explicit empty page so short-page adapters do
		// not strand durable inbox records without another wake.
	}
}

func (client *Client) presentRejectionsPage(ctx context.Context) error {
	principal := llm.AuthenticatedWorker{
		WorkerID: client.config.Worker, SessionID: "journal-rejection-presentation",
	}
	var after JournalSequence
	for {
		records, err := client.config.journal.ListRejections(ctx, after, defaultJournalPageSize)
		if err != nil {
			return &JournalError{Operation: "list rejections", Cause: err}
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			if record.Sequence == 0 || record.Sequence <= after ||
				record.EventDigest.Validate() != nil || record.ReceiptDigest.Validate() != nil ||
				record.Delivery.ValidateFor(principal) != nil || record.Receipt.Decision != llm.WorkerEventNACK ||
				record.Receipt.ValidateFor(record.Delivery) != nil {
				return &JournalError{Operation: "list rejections", Delivery: record.Receipt.Delivery, Cause: ErrJournalCorrupt}
			}
			eventDigest, eventErr := digestJournalValue(record.Delivery)
			receiptDigest, receiptErr := digestJournalValue(record.Receipt)
			if eventErr != nil || receiptErr != nil || eventDigest != record.EventDigest || receiptDigest != record.ReceiptDigest {
				return &JournalError{Operation: "list rejections", Delivery: record.Receipt.Delivery, Cause: ErrJournalCorrupt}
			}
			after = record.Sequence
			client.presentMu.Lock()
			prior, presented := client.presentRejections[record.Receipt.Delivery]
			if presented && prior != record.ReceiptDigest {
				client.presentMu.Unlock()
				return &JournalError{Operation: "present rejection", Delivery: record.Receipt.Delivery, Cause: ErrJournalCorrupt}
			}
			if !presented {
				client.presentRejections[record.Receipt.Delivery] = record.ReceiptDigest
			}
			client.presentMu.Unlock()
			if presented {
				continue
			}
			select {
			case client.rejections <- RejectedEvent{
				Delivery: llm.CloneWorkerEventDelivery(record.Delivery), Receipt: record.Receipt,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// A short non-empty page is not EOF; only an empty page terminates scan.
	}
}
