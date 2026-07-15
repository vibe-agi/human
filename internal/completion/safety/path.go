// Package safety implements gateway-side lexical defense and warning labels.
// The caller shim remains responsible for real filesystem enforcement.
package safety

import (
	"errors"
	"path"
	"slices"
	"strings"
)

type Operation string

const (
	OperationRead   Operation = "read"
	OperationWrite  Operation = "write"
	OperationDelete Operation = "delete"
	OperationRename Operation = "rename"
)

type Severity string

const (
	SeverityAllow Severity = "allow"
	SeverityWarn  Severity = "warn"
	SeverityBlock Severity = "block"
)

type PathDecision struct {
	Normalized string
	Relative   string
	Severity   Severity
	Reasons    []string
}

var sensitiveSegments = []string{
	".env", ".ssh", ".aws", ".gcp", ".azure", ".npmrc", ".pypirc",
}

func CheckVirtualPath(input string, operation Operation) (PathDecision, error) {
	if strings.TrimSpace(input) == "" {
		return PathDecision{}, errors.New("path is required")
	}
	// Backslashes are separators for Windows adapters and must not bypass the
	// virtual POSIX namespace check when the gateway runs on another OS.
	normalizedInput := strings.ReplaceAll(strings.TrimSpace(input), `\`, "/")
	if !strings.HasPrefix(normalizedInput, "/") {
		normalizedInput = "/workspace/" + normalizedInput
	}
	normalized := path.Clean(normalizedInput)
	decision := PathDecision{Normalized: normalized, Severity: SeverityAllow}
	if normalized != "/workspace" && !strings.HasPrefix(normalized, "/workspace/") {
		decision.Severity = SeverityBlock
		decision.Reasons = append(decision.Reasons, "path escapes /workspace")
		return decision, nil
	}
	decision.Relative = strings.TrimPrefix(normalized, "/workspace/")
	if normalized == "/workspace" {
		decision.Relative = "."
	}
	segments := strings.Split(strings.ToLower(decision.Relative), "/")
	if operation != OperationRead && slices.Contains(segments, ".git") {
		decision.Severity = SeverityBlock
		decision.Reasons = append(decision.Reasons, "writes to .git are forbidden")
	}
	for _, segment := range segments {
		if slices.Contains(sensitiveSegments, segment) && decision.Severity != SeverityBlock {
			decision.Severity = SeverityWarn
			decision.Reasons = append(decision.Reasons, "path may contain secrets")
			break
		}
	}
	return decision, nil
}

type CommandDecision struct {
	Severity Severity
	Reasons  []string
}

var dangerousCommandFragments = []string{
	"rm -rf", "curl|sh", "curl | sh", "wget|sh", "wget | sh", "sudo ",
	"> /dev/", "mkfs", "shutdown", "reboot",
}

func CheckCommand(command string, enabled bool) CommandDecision {
	if !enabled {
		return CommandDecision{Severity: SeverityBlock, Reasons: []string{"command capability is disabled"}}
	}
	decision := CommandDecision{Severity: SeverityAllow}
	lower := strings.ToLower(command)
	for _, fragment := range dangerousCommandFragments {
		if strings.Contains(lower, fragment) {
			decision.Severity = SeverityWarn
			decision.Reasons = append(decision.Reasons, "command matches a dangerous pattern")
			break
		}
	}
	return decision
}
