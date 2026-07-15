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
	HarnessID      string    `json:"harness_id"`
	HarnessVersion string    `json:"harness_version"`
	Read           *Tool     `json:"read,omitempty"`
	Search         *Tool     `json:"search,omitempty"`
	Write          *Tool     `json:"write,omitempty"`
	Edit           *EditTool `json:"edit,omitempty"`
	Delete         *Tool     `json:"delete,omitempty"`
	Rename         *Tool     `json:"rename,omitempty"`
	Exec           *ExecTool `json:"exec,omitempty"`
	ParallelCalls  bool      `json:"parallel_calls"`
	ErrorShape     string    `json:"error_shape"`
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
	if profile.Read == nil && profile.Search == nil && profile.Write == nil &&
		profile.Edit == nil && profile.Delete == nil && profile.Rename == nil && profile.Exec == nil {
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
