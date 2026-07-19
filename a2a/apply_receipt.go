package a2a

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

// ApplyReceiptExtensionURI activates Human's authenticated caller-side CAS
// outcome endpoint. It is deliberately separate from the Workspace metadata
// extension: advertising Artifact identities does not authorize state changes.
const ApplyReceiptExtensionURI = "https://github.com/vibe-agi/human/extensions/apply-receipt/v1"

// RecordApplyReceiptRequest is emitted only after the caller-side durable apply
// journal has reached a terminal outcome. Every identity is echoed so neither a
// retry nor a compromised UI can move a receipt to a different Artifact.
type RecordApplyReceiptRequest struct {
	CommandID        string                  `json:"commandId"`
	ReceiptID        string                  `json:"receiptId"`
	ArtifactID       string                  `json:"artifactId"`
	ArtifactDigest   workspace.Digest        `json:"artifactDigest"`
	BaseRevision     workspace.Revision      `json:"baseRevision"`
	ResultRevision   workspace.Revision      `json:"resultRevision"`
	Decision         workspace.ApplyDecision `json:"decision"`
	ObservedRevision workspace.Revision      `json:"observedRevision,omitempty"`
	Code             string                  `json:"code,omitempty"`
	Message          string                  `json:"message,omitempty"`
}

type RecordApplyReceiptResponse struct {
	ReceiptID        string                  `json:"receiptId"`
	ArtifactID       string                  `json:"artifactId"`
	ArtifactDigest   workspace.Digest        `json:"artifactDigest"`
	BaseRevision     workspace.Revision      `json:"baseRevision"`
	ResultRevision   workspace.Revision      `json:"resultRevision"`
	Decision         workspace.ApplyDecision `json:"decision"`
	ObservedRevision workspace.Revision      `json:"observedRevision,omitempty"`
	Code             string                  `json:"code,omitempty"`
	Message          string                  `json:"message,omitempty"`
	RecordedAt       time.Time               `json:"recordedAt"`
}

type applyReceiptHandler struct {
	agent     Service
	authorize AuthorizeApplyReceiptFunc
	fallback  http.Handler
}

func (handler *applyReceiptHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	taskID, matches := strings.CutSuffix(request.PathValue("idAndAction"), ":recordApplyReceipt")
	if !matches {
		handler.fallback.ServeHTTP(response, request)
		return
	}
	if !extensionRequested(request, ApplyReceiptExtensionURI) {
		writeProtocolError(
			response, http.StatusBadRequest, "FAILED_PRECONDITION",
			sdk.ErrExtensionSupportRequired, "apply receipt extension was not activated",
		)
		return
	}
	principal, err := requirePrincipal(request.Context())
	if err != nil {
		writeRequestHandlerError(response, err)
		return
	}
	if taskID == "" {
		writeProtocolError(response, http.StatusBadRequest, "INVALID_ARGUMENT", sdk.ErrInvalidParams, "task id is required")
		return
	}
	var payload RecordApplyReceiptRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeProtocolError(response, http.StatusBadRequest, "INVALID_ARGUMENT", sdk.ErrInvalidParams, "apply receipt payload is invalid")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeProtocolError(response, http.StatusBadRequest, "INVALID_ARGUMENT", sdk.ErrInvalidParams, "apply receipt payload has trailing data")
		return
	}
	ref, err := handler.agent.ResolveTask(request.Context(), principal.Authority, agent.TaskID(taskID))
	if err != nil {
		writeRequestHandlerError(response, mapAgentError(err))
		return
	}
	task, err := handler.agent.GetTask(request.Context(), ref)
	if err != nil {
		writeRequestHandlerError(response, mapAgentError(err))
		return
	}
	if handler.authorize == nil {
		writeProtocolError(response, http.StatusForbidden, "PERMISSION_DENIED", sdk.ErrUnauthorized, "permission denied")
		return
	}
	if err := handler.authorize(request.Context(), principal, task, &payload); err != nil {
		writeApplyReceiptAuthorizationError(response, err)
		return
	}
	if task.State != agent.TaskCompleted || task.Artifact == nil || string(task.Artifact.ID) != payload.ArtifactID {
		writeProtocolError(response, http.StatusConflict, "FAILED_PRECONDITION", sdk.ErrInvalidParams, "task has no matching published Artifact")
		return
	}
	receipt, err := handler.agent.RecordApplyReceipt(request.Context(), agent.RecordApplyReceiptCommand{
		CommandID: agent.CommandID(payload.CommandID), Receipt: agent.ApplyReceiptID(payload.ReceiptID),
		Artifact: *task.Artifact, ArtifactDigest: payload.ArtifactDigest,
		BaseRevision: payload.BaseRevision, ResultRevision: payload.ResultRevision,
		Decision: payload.Decision, ObservedRevision: payload.ObservedRevision,
		Code: payload.Code, Message: payload.Message,
	})
	if err != nil {
		writeRequestHandlerError(response, mapAgentError(err))
		return
	}
	response.Header().Set("Content-Type", "application/a2a+json")
	_ = json.NewEncoder(response).Encode(RecordApplyReceiptResponse{
		ReceiptID: string(receipt.ID), ArtifactID: string(receipt.Artifact.ID),
		ArtifactDigest: receipt.ArtifactDigest, BaseRevision: receipt.BaseRevision,
		ResultRevision: receipt.ResultRevision, Decision: receipt.Decision,
		ObservedRevision: receipt.ObservedRevision, Code: receipt.Code,
		Message: receipt.Message, RecordedAt: receipt.RecordedAt,
	})
}

func writeApplyReceiptAuthorizationError(response http.ResponseWriter, err error) {
	code, retry, classified := framework.FaultInfo(err)
	switch {
	case classified && code == framework.CodeUnauthenticated && retry == framework.RetryNever:
		writeProtocolError(response, http.StatusUnauthorized, "UNAUTHENTICATED", sdk.ErrUnauthenticated, "authentication required")
	case classified && code == framework.CodeForbidden && retry == framework.RetryNever:
		writeProtocolError(response, http.StatusForbidden, "PERMISSION_DENIED", sdk.ErrUnauthorized, "permission denied")
	default:
		// An unclassified policy error is an infrastructure failure, not proof that
		// the caller is forbidden. Keep the receipt retryable and never expose the
		// policy backend's cause or safe embedding-only Fault message.
		response.Header().Set("Retry-After", "1")
		writeProtocolError(response, http.StatusServiceUnavailable, "UNAVAILABLE", sdk.ErrServerError, "authorization service unavailable")
	}
}

func extensionRequested(request *http.Request, uri string) bool {
	for _, requested := range parseExtensionHeaders(request.Header.Values(sdk.SvcParamExtensions)) {
		if requested == uri {
			return true
		}
	}
	return false
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("extra JSON value")
		}
		return err
	}
	return nil
}

func writeRequestHandlerError(response http.ResponseWriter, err error) {
	var protocolErr *sdk.Error
	if !errors.As(err, &protocolErr) {
		protocolErr = sdk.NewError(sdk.ErrInternalError, "internal HumanAgent error")
	}
	status := http.StatusInternalServerError
	grpcStatus := "INTERNAL"
	switch {
	case errors.Is(protocolErr, sdk.ErrUnauthenticated):
		status, grpcStatus = http.StatusUnauthorized, "UNAUTHENTICATED"
	case errors.Is(protocolErr, sdk.ErrUnauthorized):
		status, grpcStatus = http.StatusForbidden, "PERMISSION_DENIED"
	case errors.Is(protocolErr, sdk.ErrTaskNotFound):
		status, grpcStatus = http.StatusNotFound, "NOT_FOUND"
	case errors.Is(protocolErr, sdk.ErrInvalidParams), errors.Is(protocolErr, sdk.ErrInvalidRequest):
		status, grpcStatus = http.StatusBadRequest, "INVALID_ARGUMENT"
	}
	writeA2AError(response, status, grpcStatus, protocolErr)
}
