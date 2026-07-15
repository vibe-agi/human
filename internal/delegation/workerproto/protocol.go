// Package workerproto defines the private, versioned delegation transport
// between humand and the human worker. It is independent from completion mode.
package workerproto

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vibe-agi/human/internal/delegation"
)

const Version = "1"

type MessageType string

const (
	MessageHello         MessageType = "hello"
	MessageSnapshot      MessageType = "snapshot"
	MessageTaskRemoved   MessageType = "task_removed"
	MessageCommand       MessageType = "command"
	MessageCommandResult MessageType = "command_result"
	MessageAck           MessageType = "ack"
	MessageError         MessageType = "error"
)

type Envelope struct {
	Version string          `json:"version"`
	Seq     uint64          `json:"seq"`
	Ack     uint64          `json:"ack"`
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewEnvelope(messageType MessageType, payload any) (Envelope, error) {
	var raw json.RawMessage
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return Envelope{}, err
		}
		raw = encoded
	}
	return Envelope{Version: Version, Type: messageType, Payload: raw}, nil
}

func (envelope Envelope) Validate() error {
	if envelope.Version != Version {
		return fmt.Errorf("unsupported delegation worker protocol version %q", envelope.Version)
	}
	if envelope.Seq == 0 {
		return errors.New("delegation worker message seq must be positive")
	}
	switch envelope.Type {
	case MessageHello, MessageSnapshot, MessageTaskRemoved, MessageCommand,
		MessageCommandResult, MessageAck, MessageError:
		return nil
	default:
		return fmt.Errorf("unsupported delegation worker message type %q", envelope.Type)
	}
}

type Hello struct {
	WorkerID string `json:"worker_id"`
}

type SnapshotReason string

const (
	ReasonRecovery  SnapshotReason = "recovery"
	ReasonSubmitted SnapshotReason = "submitted"
	ReasonMessage   SnapshotReason = "message"
	ReasonInterrupt SnapshotReason = "interrupt"
	ReasonRewind    SnapshotReason = "rewind"
	ReasonExec      SnapshotReason = "exec"
	ReasonState     SnapshotReason = "state"
)

type Snapshot struct {
	EventID  string              `json:"event_id"`
	Reason   SnapshotReason      `json:"reason"`
	Snapshot delegation.Snapshot `json:"snapshot"`
}

type TaskRemoved struct {
	EventID string `json:"event_id"`
	TaskID  string `json:"task_id"`
}

type CommandKind string

const (
	CommandAccept        CommandKind = "accept"
	CommandReject        CommandKind = "reject"
	CommandDeliver       CommandKind = "deliver"
	CommandComplete      CommandKind = "complete"
	CommandFail          CommandKind = "fail"
	CommandConfirmRewind CommandKind = "confirm_rewind"
	CommandRejectRewind  CommandKind = "reject_rewind"
	CommandExec          CommandKind = "exec"
)

type Delivery struct {
	ArtifactID        string `json:"artifact_id"`
	ArtifactMediaType string `json:"artifact_media_type"`
	ArtifactData      []byte `json:"artifact_data"`
	ArtifactMetadata  []byte `json:"artifact_metadata,omitempty"`
}

// ExecRequest deliberately omits worker identity. The server derives it only
// from the authenticated worker token before crossing the authority boundary.
type ExecRequest struct {
	RequestID string `json:"request_id"`
	Command   string `json:"command"`
	CWD       string `json:"cwd,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
	Reason    string `json:"reason"`
}

// Command.EventID is the durable idempotency key. ExpectedRevision is the
// authority CAS; authenticated worker identity is never accepted from payload.
type Command struct {
	EventID          string       `json:"event_id"`
	Kind             CommandKind  `json:"kind"`
	TaskID           string       `json:"task_id"`
	ExpectedRevision int64        `json:"expected_revision"`
	Data             []byte       `json:"data,omitempty"`
	Delivery         *Delivery    `json:"delivery,omitempty"`
	Exec             *ExecRequest `json:"exec,omitempty"`
}

func (command Command) Validate() error {
	if strings.TrimSpace(command.EventID) == "" || command.EventID != strings.TrimSpace(command.EventID) ||
		strings.TrimSpace(command.TaskID) == "" || command.TaskID != strings.TrimSpace(command.TaskID) ||
		command.ExpectedRevision < 1 {
		return errors.New("canonical event id, canonical task id, and positive expected revision are required")
	}
	switch command.Kind {
	case CommandAccept, CommandReject, CommandComplete, CommandFail,
		CommandConfirmRewind, CommandRejectRewind:
		if command.Delivery != nil || command.Exec != nil {
			return errors.New("lifecycle command cannot carry delivery or exec payload")
		}
	case CommandDeliver:
		if command.Exec != nil || command.Delivery == nil || strings.TrimSpace(command.Delivery.ArtifactID) == "" ||
			command.Delivery.ArtifactID != strings.TrimSpace(command.Delivery.ArtifactID) ||
			strings.TrimSpace(command.Delivery.ArtifactMediaType) == "" ||
			command.Delivery.ArtifactMediaType != strings.TrimSpace(command.Delivery.ArtifactMediaType) {
			return errors.New("deliver requires artifact id and media type")
		}
	case CommandExec:
		if command.Delivery != nil || command.Exec == nil {
			return errors.New("exec requires an exec payload only")
		}
		exec := command.Exec
		if strings.TrimSpace(exec.RequestID) == "" || exec.RequestID != strings.TrimSpace(exec.RequestID) ||
			len(exec.RequestID) > 128 || strings.TrimSpace(exec.Command) == "" || len(exec.Command) > 64<<10 ||
			strings.IndexByte(exec.Command, 0) >= 0 || exec.CWD != strings.TrimSpace(exec.CWD) ||
			len(exec.CWD) > 4<<10 || strings.TrimSpace(exec.Reason) == "" ||
			exec.Reason != strings.TrimSpace(exec.Reason) || len(exec.Reason) > 8<<10 ||
			exec.TimeoutMS < 0 || exec.TimeoutMS > 60*60*1000 {
			return errors.New("exec request is non-canonical or exceeds protocol limits")
		}
	default:
		return fmt.Errorf("unsupported delegation worker command %q", command.Kind)
	}
	return nil
}

func (snapshot Snapshot) Validate() error {
	if strings.TrimSpace(snapshot.EventID) == "" || strings.TrimSpace(snapshot.Snapshot.Task.ID) == "" ||
		snapshot.Snapshot.Task.Revision < 1 {
		return errors.New("snapshot event, task, and revision identity are required")
	}
	switch snapshot.Reason {
	case ReasonRecovery, ReasonSubmitted, ReasonMessage, ReasonInterrupt, ReasonRewind, ReasonExec, ReasonState:
		return nil
	default:
		return fmt.Errorf("unsupported delegation snapshot reason %q", snapshot.Reason)
	}
}

func (removed TaskRemoved) Validate() error {
	if strings.TrimSpace(removed.EventID) == "" || strings.TrimSpace(removed.TaskID) == "" {
		return errors.New("task removal event and task identity are required")
	}
	return nil
}

type CommandResult struct {
	EventID    string                       `json:"event_id"`
	Kind       CommandKind                  `json:"kind"`
	Transition *delegation.TransitionResult `json:"transition,omitempty"`
	Delivery   *delegation.DeliveryResult   `json:"delivery,omitempty"`
	Exec       *delegation.ExecResult       `json:"exec,omitempty"`
	Error      *Error                       `json:"error,omitempty"`
	Replay     bool                         `json:"replay,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (result CommandResult) Validate() error {
	if strings.TrimSpace(result.EventID) == "" || result.EventID != strings.TrimSpace(result.EventID) {
		return errors.New("command result event id is required")
	}
	if result.Error != nil {
		if strings.TrimSpace(result.Error.Code) == "" || result.Transition != nil || result.Delivery != nil || result.Exec != nil {
			return errors.New("error command result has an invalid payload")
		}
		return nil
	}
	switch result.Kind {
	case CommandDeliver:
		if result.Delivery == nil || result.Transition != nil || result.Exec != nil {
			return errors.New("deliver command result requires delivery only")
		}
	case CommandExec:
		if result.Exec == nil || result.Transition != nil || result.Delivery != nil {
			return errors.New("exec command result requires exec only")
		}
	case CommandAccept, CommandReject, CommandComplete, CommandFail,
		CommandConfirmRewind, CommandRejectRewind:
		if result.Transition == nil || result.Delivery != nil || result.Exec != nil {
			return errors.New("lifecycle command result requires transition only")
		}
	default:
		return fmt.Errorf("unsupported command result kind %q", result.Kind)
	}
	return nil
}
