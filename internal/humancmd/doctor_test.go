package humancmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/userdata"
)

func TestDoctorJSONWarnsForFirstRunWithoutFailing(t *testing.T) {
	dataRoot := t.TempDir()
	if err := os.Chmod(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	dependencies := doctorDependencies{
		openCode: func(context.Context) doctorOpenCodeResult {
			return doctorOpenCodeResult{Found: true, Path: "/test/opencode", Version: adapter.OpenCodeVersion}
		},
		health: func(context.Context, string) doctorHealthResult {
			return doctorHealthResult{Running: false, Err: context.DeadlineExceeded}
		},
	}
	command := newDoctorCommandWithDependencies(dependencies)
	command.SilenceErrors = true
	command.SilenceUsage = true
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--workspace", ".", "--json"})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("warnings made doctor fail: %v\n%s", err, output.String())
	}
	var report doctorReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatalf("decode doctor JSON %q: %v", output.String(), err)
	}
	if !report.OK || report.Summary.Fail != 0 || report.Summary.Warn != 2 {
		t.Fatalf("first-run report = %+v", report)
	}
	if report.DataRoot != dataRoot || !strings.HasPrefix(report.WorkspaceData, dataRoot+string(filepath.Separator)) {
		t.Fatalf("doctor paths escaped user data: %+v", report)
	}
	entries, err := os.ReadDir(report.WorkspaceData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("doctor left probe files behind: %v", entries)
	}
}

func TestDoctorValidatesCredentialJournalWithoutPrintingSecrets(t *testing.T) {
	dataRoot := t.TempDir()
	if err := os.Chmod(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	workspace, err := userdata.ResolveGitWorkspace(".")
	if err != nil {
		t.Fatal(err)
	}
	credentialPath, err := userdata.WorkspacePath("local", workspace, "credentials.json")
	if err != nil {
		t.Fatal(err)
	}
	const callerSecret = "caller-secret-must-never-print"
	const workerSecret = "worker-secret-must-never-print"
	if err := writeLocalCredentials(credentialPath, localCredentialFile{
		Version: localCredentialVersion,
		Active: &localCredentialPair{
			Caller: localCredential{Type: gateway.PrincipalCaller, SubjectID: "local-caller", KeyID: "caller-key", Secret: callerSecret},
			Worker: localCredential{Type: gateway.PrincipalWorker, SubjectID: "local-worker", KeyID: "worker-key", Secret: workerSecret},
		},
	}); err != nil {
		t.Fatal(err)
	}
	report := runDoctor(context.Background(), ".", defaultDoctorHealthURL, false, doctorDependencies{
		openCode: func(context.Context) doctorOpenCodeResult {
			return doctorOpenCodeResult{Found: true, Path: "/test/opencode", Version: adapter.OpenCodeVersion}
		},
		health: func(context.Context, string) doctorHealthResult { return doctorHealthResult{Running: true} },
	})
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK || report.Summary.Fail != 0 || report.Summary.Warn != 0 {
		t.Fatalf("ready doctor report = %+v", report)
	}
	if bytes.Contains(encoded, []byte(callerSecret)) || bytes.Contains(encoded, []byte(workerSecret)) {
		t.Fatalf("doctor leaked a credential: %s", encoded)
	}
}

func TestDoctorFailsForUnsupportedInstalledOpenCode(t *testing.T) {
	dataRoot := t.TempDir()
	if err := os.Chmod(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	command := newDoctorCommandWithDependencies(doctorDependencies{
		openCode: func(context.Context) doctorOpenCodeResult {
			return doctorOpenCodeResult{Found: true, Path: "/test/opencode", Version: "9.9.9"}
		},
		health: func(context.Context, string) doctorHealthResult { return doctorHealthResult{Running: false} },
	})
	command.SilenceErrors = true
	command.SilenceUsage = true
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--json", "--require-opencode"})
	err := command.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed check") {
		t.Fatalf("unsupported OpenCode error = %v", err)
	}
	var report doctorReport
	if decodeErr := json.Unmarshal(output.Bytes(), &report); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if report.OK || report.Summary.Fail != 1 {
		t.Fatalf("unsupported OpenCode report = %+v", report)
	}
}

func TestDoctorFailsWhenOpenCodeIsMissing(t *testing.T) {
	dataRoot := t.TempDir()
	if err := os.Chmod(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	report := runDoctor(context.Background(), ".", defaultDoctorHealthURL, true, doctorDependencies{
		openCode: func(context.Context) doctorOpenCodeResult { return doctorOpenCodeResult{} },
		health:   func(context.Context, string) doctorHealthResult { return doctorHealthResult{Running: false} },
	})
	if report.OK || report.Summary.Fail != 1 {
		t.Fatalf("missing OpenCode report = %+v", report)
	}
}

func TestDoctorWarnsForOpenCodeMismatchUnlessRequired(t *testing.T) {
	dataRoot := t.TempDir()
	if err := os.Chmod(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	report := runDoctor(context.Background(), ".", defaultDoctorHealthURL, false, doctorDependencies{
		openCode: func(context.Context) doctorOpenCodeResult {
			return doctorOpenCodeResult{Found: true, Path: "/test/opencode", Version: "9.9.9"}
		},
		health: func(context.Context, string) doctorHealthResult { return doctorHealthResult{Running: true} },
	})
	if !report.OK || report.Summary.Fail != 0 {
		t.Fatalf("generic doctor rejected a non-OpenCode product setup: %+v", report)
	}
	for _, check := range report.Checks {
		if check.ID == "opencode" && check.Status != doctorWarn {
			t.Fatalf("OpenCode mismatch status = %q", check.Status)
		}
	}
}

func TestProbeHealthDistinguishesHealthyAndUnhealthyHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthy" {
			_, _ = response.Write([]byte(`{"status":"ok"}`))
			return
		}
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if got := probeHealth(context.Background(), server.URL+"/healthy"); !got.Running || got.Err != nil {
		t.Fatalf("healthy probe = %+v", got)
	}
	if got := probeHealth(context.Background(), server.URL+"/unhealthy"); !got.Running || got.Err == nil {
		t.Fatalf("unhealthy probe = %+v", got)
	}
}

func TestProbeHealthRejectsInvalidExplicitURLAsConfigurationFailure(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"not-a-url", "file:///tmp/health", "http://user:secret@example.test/health"} {
		got := probeHealth(context.Background(), value)
		if !got.Running || got.Err == nil {
			t.Fatalf("invalid health URL %q = %+v, want non-retryable diagnostic failure", value, got)
		}
	}
}
