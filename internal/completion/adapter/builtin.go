package adapter

const (
	HumanShimID      = "human-shim"
	HumanShimVersion = "1"
)

// NewDefaultRegistry returns the exact, versioned adapters shipped with the
// daemon. Completion mode deliberately starts with the project-owned caller shim;
// real harness adapters are added only after their golden fixtures are
// captured and reviewed.
func NewDefaultRegistry() *Registry {
	registry := NewRegistry()
	if err := registry.Register(HumanShimProfile()); err != nil {
		panic(err)
	}
	return registry
}

func HumanShimProfile() Profile {
	return Profile{
		HarnessID:      HumanShimID,
		HarnessVersion: HumanShimVersion,
		Read: &Tool{Name: "human_read_file", Args: map[string]string{
			"path": "path",
		}},
		Search: &Tool{Name: "human_search", Args: map[string]string{
			"query": "query", "path": "path",
		}},
		Write: &Tool{Name: "human_write_file", Args: map[string]string{
			"path": "path", "content": "content", "expected_sha256": "expected_sha256",
		}},
		Edit: &EditTool{
			Tool: Tool{Name: "human_edit_file", Args: map[string]string{
				"path": "path", "old": "old_string", "new": "new_string", "expected_sha256": "expected_sha256",
			}},
			Semantics: EditExact,
			OldField:  "old_string",
			NewField:  "new_string",
		},
		Delete: &Tool{Name: "human_delete_file", Args: map[string]string{
			"path": "path", "expected_sha256": "expected_sha256",
		}},
		Rename: &Tool{Name: "human_rename_file", Args: map[string]string{
			"from": "from", "to": "to", "expected_sha256": "expected_sha256",
		}},
		Exec: &ExecTool{
			Tool: Tool{Name: "human_exec", Args: map[string]string{
				"command": "command", "cwd": "cwd", "timeout_ms": "timeout_ms",
			}},
			CWDField: "cwd", TimeoutField: "timeout_ms", Approval: "caller",
		},
		ParallelCalls: true,
		ErrorShape:    "{content:string,is_error:boolean}",
	}
}
