// Package adapter contains explicit, versioned harness capability profiles.
// It never infers write or execution capabilities from a JSON schema shape.
package adapter

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type EditSemantics string

const (
	EditExact EditSemantics = "exact"
	EditFuzzy EditSemantics = "fuzzy"
	EditLine  EditSemantics = "line"
)

// PathStyle describes how a harness addresses files. Mirror code uses this
// declaration instead of guessing from a field name or JSON schema.
type PathStyle string

const (
	PathWorkspaceVirtual PathStyle = "workspace_virtual"
	PathAbsolute         PathStyle = "absolute"
)

// ResultCodec names an exact, versioned tool-result contract. A profile may
// omit it when it is not eligible for workspace reconciliation.
type ResultCodec string

const (
	ResultHumanShimV1   ResultCodec = "human_shim_v1"
	ResultOpenCode11718 ResultCodec = "opencode_1_17_18"
)

// ToolAuthorization classifies caller-native tools that do not need a
// workspace semantic mapping. Standard tools are known not to grant command,
// network, sub-agent, or otherwise open-ended authority. Privileged tools (and
// tools an exact profile does not classify) require the task's explicit active
// capability opt-in before Human may return them.
//
// This is deliberately separate from Tool mappings: a native tool such as
// todowrite can be safe to return without being usable for mirror
// reconciliation, while task/webfetch must not inherit authority merely from
// appearing in the caller's schema.
type ToolAuthorization string

const (
	ToolAuthorizationStandard   ToolAuthorization = "standard"
	ToolAuthorizationPrivileged ToolAuthorization = "privileged"
)

type Tool struct {
	Name string            `json:"name"`
	Args map[string]string `json:"args,omitempty"`
}

type EditTool struct {
	Tool
	Semantics EditSemantics `json:"semantics"`
	OldField  string        `json:"old_field"`
	NewField  string        `json:"new_field"`
}

type ExecTool struct {
	Tool
	CWDField     string `json:"cwd_field,omitempty"`
	TimeoutField string `json:"timeout_field,omitempty"`
	Approval     string `json:"approval,omitempty"`
}

type Profile struct {
	HarnessID      string                       `json:"harness_id"`
	HarnessVersion string                       `json:"harness_version"`
	PathStyle      PathStyle                    `json:"path_style,omitempty"`
	ResultCodec    ResultCodec                  `json:"result_codec,omitempty"`
	Read           *Tool                        `json:"read,omitempty"`
	Search         *Tool                        `json:"search,omitempty"`
	Write          *Tool                        `json:"write,omitempty"`
	Edit           *EditTool                    `json:"edit,omitempty"`
	Delete         *Tool                        `json:"delete,omitempty"`
	Rename         *Tool                        `json:"rename,omitempty"`
	Exec           *ExecTool                    `json:"exec,omitempty"`
	NativeTools    map[string]ToolAuthorization `json:"native_tools,omitempty"`
	ParallelCalls  bool                         `json:"parallel_calls"`
	ErrorShape     string                       `json:"error_shape"`
}

func (profile Profile) Key() string {
	return profile.HarnessID + "@" + profile.HarnessVersion
}

func (profile Profile) Validate() error {
	if strings.TrimSpace(profile.HarnessID) == "" || strings.TrimSpace(profile.HarnessVersion) == "" {
		return errors.New("harness id and version are required")
	}
	if strings.TrimSpace(profile.ErrorShape) == "" {
		return errors.New("error shape is required")
	}
	switch profile.PathStyle {
	case "", PathWorkspaceVirtual, PathAbsolute:
	default:
		return fmt.Errorf("unsupported path style %q", profile.PathStyle)
	}
	switch profile.ResultCodec {
	case "", ResultHumanShimV1, ResultOpenCode11718:
	default:
		return fmt.Errorf("unsupported result codec %q", profile.ResultCodec)
	}
	if (profile.PathStyle == "") != (profile.ResultCodec == "") {
		return errors.New("workspace path style and result codec must be declared together")
	}
	switch profile.ResultCodec {
	case ResultHumanShimV1:
		if profile.Key() != HumanShimID+"@"+HumanShimVersion || profile.PathStyle != PathWorkspaceVirtual {
			return fmt.Errorf("result codec %q is reserved for %s@%s with %q paths",
				profile.ResultCodec, HumanShimID, HumanShimVersion, PathWorkspaceVirtual)
		}
	case ResultOpenCode11718:
		if profile.Key() != OpenCodeID+"@"+OpenCodeVersion || profile.PathStyle != PathAbsolute {
			return fmt.Errorf("result codec %q is reserved for %s@%s with %q paths",
				profile.ResultCodec, OpenCodeID, OpenCodeVersion, PathAbsolute)
		}
	}
	tools := []*Tool{profile.Read, profile.Search, profile.Write, profile.Delete, profile.Rename}
	seen := make(map[string]struct{})
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		if err := validateTool(tool.Name, seen); err != nil {
			return err
		}
	}
	if profile.Edit != nil {
		if err := validateTool(profile.Edit.Name, seen); err != nil {
			return err
		}
		switch profile.Edit.Semantics {
		case EditExact, EditFuzzy, EditLine:
		default:
			return fmt.Errorf("unsupported edit semantics %q", profile.Edit.Semantics)
		}
		if profile.Edit.OldField == "" || profile.Edit.NewField == "" {
			return errors.New("edit old and new fields are required")
		}
	}
	if profile.Exec != nil {
		if err := validateTool(profile.Exec.Name, seen); err != nil {
			return err
		}
	}
	for name, authorization := range profile.NativeTools {
		if err := validateTool(name, seen); err != nil {
			return err
		}
		switch authorization {
		case ToolAuthorizationStandard, ToolAuthorizationPrivileged:
		default:
			return fmt.Errorf("native tool %q has unsupported authorization %q", name, authorization)
		}
	}
	if profile.Read == nil && profile.Search == nil && profile.Write == nil &&
		profile.Edit == nil && profile.Delete == nil && profile.Rename == nil && profile.Exec == nil &&
		len(profile.NativeTools) == 0 {
		return errors.New("profile declares no capabilities")
	}
	return nil
}

// AllowsTool reports whether name is explicitly mapped by this exact harness
// profile. It deliberately performs no schema-shape inference.
func (profile Profile) AllowsTool(name string) bool {
	for _, tool := range []*Tool{profile.Read, profile.Search, profile.Write, profile.Delete, profile.Rename} {
		if tool != nil && tool.Name == name {
			return true
		}
	}
	return profile.Edit != nil && profile.Edit.Name == name || profile.Exec != nil && profile.Exec.Name == name
}

func (profile Profile) IsExecTool(name string) bool {
	return profile.Exec != nil && profile.Exec.Name == name
}

// AuthorizeTool reports the reviewed authorization class for name. Exact
// filesystem mappings are standard workspace capabilities, while the mapped
// command tool is privileged. A false result is intentionally not equivalent
// to safe: unclassified native/custom/MCP tools require explicit privileged
// authorization at the gateway, where caller declaration is checked
// separately.
func (profile Profile) AuthorizeTool(name string) (ToolAuthorization, bool) {
	if profile.IsExecTool(name) {
		return ToolAuthorizationPrivileged, true
	}
	if profile.AllowsTool(name) {
		return ToolAuthorizationStandard, true
	}
	authorization, ok := profile.NativeTools[name]
	return authorization, ok
}

func validateTool(name string, seen map[string]struct{}) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("tool name is required")
	}
	if _, exists := seen[name]; exists {
		return fmt.Errorf("tool name %q is declared more than once", name)
	}
	seen[name] = struct{}{}
	return nil
}

type Registry struct {
	mu       sync.RWMutex
	profiles map[string]Profile
}

func NewRegistry() *Registry {
	return &Registry{profiles: make(map[string]Profile)}
}

func (registry *Registry) Register(profile Profile) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.profiles[profile.Key()]; exists {
		return fmt.Errorf("adapter profile %q already registered", profile.Key())
	}
	registry.profiles[profile.Key()] = profile
	return nil
}

func (registry *Registry) Resolve(harnessID, harnessVersion string) (Profile, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	profile, ok := registry.profiles[harnessID+"@"+harnessVersion]
	return profile, ok
}
