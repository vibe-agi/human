package a2a

import (
	"context"
	"errors"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func protocolError(kind error, message string) error {
	return sdk.NewError(kind, message)
}

func mapAgentError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var transition *agent.TransitionError
	if errors.As(err, &transition) {
		return protocolError(sdk.ErrInvalidParams, transition.Error())
	}
	switch {
	case errors.Is(err, agent.ErrNotFound), errors.Is(err, agent.ErrArtifactNotFound):
		return protocolError(sdk.ErrTaskNotFound, "task was not found")
	case errors.Is(err, agent.ErrInvalidArgument),
		errors.Is(err, agent.ErrIdempotencyConflict),
		errors.Is(err, agent.ErrRevisionConflict),
		errors.Is(err, agent.ErrTaskConflict),
		errors.Is(err, agent.ErrMessageConflict),
		errors.Is(err, agent.ErrWorkspaceConflict),
		errors.Is(err, agent.ErrReceiptConflict):
		return protocolError(sdk.ErrInvalidParams, err.Error())
	default:
		return protocolError(sdk.ErrInternalError, "HumanAgent could not complete the operation")
	}
}

func mapResolverError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var typed *sdk.Error
	if errors.As(err, &typed) {
		return typed
	}
	if errors.Is(err, agent.ErrInvalidArgument) {
		return protocolError(sdk.ErrInvalidParams, "workspace could not be resolved")
	}
	return protocolError(sdk.ErrInternalError, "HumanAgent could not resolve the workspace")
}

func mapCancelError(err error) error {
	var transition *agent.TransitionError
	if errors.As(err, &transition) || errors.Is(err, agent.ErrRevisionConflict) {
		return protocolError(sdk.ErrTaskNotCancelable, "task cannot be canceled")
	}
	return mapAgentError(err)
}
