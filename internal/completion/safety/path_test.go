package safety

import "testing"

func TestCheckVirtualPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		path      string
		operation Operation
		severity  Severity
		relative  string
	}{
		{name: "relative", path: "src/main.go", operation: OperationWrite, severity: SeverityAllow, relative: "src/main.go"},
		{name: "absolute virtual", path: "/workspace/src/main.go", operation: OperationRead, severity: SeverityAllow, relative: "src/main.go"},
		{name: "posix escape", path: "/workspace/../../etc/passwd", operation: OperationRead, severity: SeverityBlock},
		{name: "windows escape", path: `\workspace\..\..\Windows\system.ini`, operation: OperationRead, severity: SeverityBlock},
		{name: "git write", path: "/workspace/.GIT/hooks/pre-commit", operation: OperationWrite, severity: SeverityBlock},
		{name: "git read", path: "/workspace/.git/config", operation: OperationRead, severity: SeverityAllow, relative: ".git/config"},
		{name: "secret warning", path: "/workspace/.ssh/id_ed25519", operation: OperationRead, severity: SeverityWarn, relative: ".ssh/id_ed25519"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := CheckVirtualPath(tt.path, tt.operation)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Severity != tt.severity || (tt.relative != "" && decision.Relative != tt.relative) {
				t.Fatalf("decision = %+v", decision)
			}
		})
	}
}

func TestCommandDefaultsClosed(t *testing.T) {
	t.Parallel()
	if decision := CheckCommand("go test ./...", false); decision.Severity != SeverityBlock {
		t.Fatalf("disabled command decision = %+v", decision)
	}
	if decision := CheckCommand("go test ./...", true); decision.Severity != SeverityAllow {
		t.Fatalf("safe command decision = %+v", decision)
	}
	if decision := CheckCommand("sudo rm -rf /", true); decision.Severity != SeverityWarn {
		t.Fatalf("dangerous command decision = %+v", decision)
	}
}
