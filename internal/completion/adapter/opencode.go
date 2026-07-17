package adapter

const (
	OpenCodeID      = "opencode"
	OpenCodeVersion = "1.17.18"
)

// OpenCode11718Profile describes only the native tools observed from OpenCode
// 1.17.18. Keep the version in the constructor name so a future schema change
// is introduced as a separate profile instead of silently widening this one.
func OpenCode11718Profile() Profile {
	return Profile{
		HarnessID:      OpenCodeID,
		HarnessVersion: OpenCodeVersion,
		PathStyle:      PathAbsolute,
		ResultCodec:    ResultOpenCode11718,
		Read: &Tool{Name: "read", Args: map[string]string{
			"path": "filePath",
		}},
		Write: &Tool{Name: "write", Args: map[string]string{
			"path": "filePath", "content": "content",
		}},
		Edit: &EditTool{
			Tool: Tool{Name: "edit", Args: map[string]string{
				"path": "filePath", "old": "oldString", "new": "newString",
			}},
			Semantics: EditExact,
			OldField:  "oldString",
			NewField:  "newString",
		},
		Exec: &ExecTool{
			Tool: Tool{Name: "bash", Args: map[string]string{
				"command": "command", "cwd": "workdir", "timeout_ms": "timeout",
			}},
			CWDField: "workdir", TimeoutField: "timeout",
		},
		// These names are native to the exact captured harness but are not
		// mirror semantic mappings. Keep benign plan/read helpers usable while
		// requiring the task-wide explicit active-capability opt-in for tools
		// that can spawn agents/processes, reach the network, or escape the
		// workspace authority boundary. Unrecognized custom/MCP tools are
		// treated like privileged tools by the gateway as well.
		NativeTools: map[string]ToolAuthorization{
			"glob":               ToolAuthorizationStandard,
			"grep":               ToolAuthorizationStandard,
			"list":               ToolAuthorizationStandard,
			"question":           ToolAuthorizationStandard,
			"todoread":           ToolAuthorizationStandard,
			"todowrite":          ToolAuthorizationStandard,
			"codesearch":         ToolAuthorizationPrivileged,
			"doom_loop":          ToolAuthorizationPrivileged,
			"external_directory": ToolAuthorizationPrivileged,
			"lsp":                ToolAuthorizationPrivileged,
			"skill":              ToolAuthorizationPrivileged,
			"task":               ToolAuthorizationPrivileged,
			"webfetch":           ToolAuthorizationPrivileged,
			"websearch":          ToolAuthorizationPrivileged,
		},
		ParallelCalls: true,
		ErrorShape:    "OpenAI Chat tool message {tool_call_id:string,content:string}",
	}
}
