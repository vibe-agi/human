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

func TestDoctorValidatesCallerCredentialWithoutPrintingSecret(t *testing.T) {
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
	if err := writePublicCredentials(credentialPath, callerSecret); err != nil {
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
	if bytes.Contains(encoded, []byte(callerSecret)) {
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
			_, _ = response.Write([]byte(`{"status":"ok","database":{"status":"ok"},"recovery":{"complete":true},"workers":{"online":2,"has_online":true}}`))
			return
		}
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if got := probeHealth(context.Background(), server.URL+"/healthy"); !got.Running || got.Err != nil ||
		!got.Observed || got.DatabaseStatus != "ok" || !got.RecoveryComplete ||
		got.OnlineWorkers != 2 || !got.HasOnlineWorker {
		t.Fatalf("healthy probe = %+v", got)
	}
	if got := probeHealth(context.Background(), server.URL+"/unhealthy"); !got.Running || got.Err == nil {
		t.Fatalf("unhealthy probe = %+v", got)
	}
}

func TestProbeHealthAcceptsReadyGatewayWithoutWorker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"status":"ok","database":{"status":"ok"},"recovery":{"complete":true},"workers":{"online":0,"has_online":false}}`))
	}))
	defer server.Close()
	got := probeHealth(context.Background(), server.URL+"/readyz")
	if !got.Running || got.Err != nil || !got.Observed || got.HasOnlineWorker || got.OnlineWorkers != 0 {
		t.Fatalf("ready gateway without worker probe = %+v", got)
	}
}

func TestDoctorWarnsButDoesNotFailWhenServiceHasNoWorker(t *testing.T) {
	dataRoot := t.TempDir()
	if err := os.Chmod(dataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	report := runDoctor(context.Background(), ".", defaultDoctorHealthURL, false, doctorDependencies{
		openCode: func(context.Context) doctorOpenCodeResult {
			return doctorOpenCodeResult{Found: true, Path: "/test/opencode", Version: adapter.OpenCodeVersion}
		},
		health: func(context.Context, string) doctorHealthResult {
			return doctorHealthResult{
				Running: true, Observed: true, DatabaseStatus: "ok", RecoveryComplete: true,
			}
		},
	})
	if !report.OK || report.Summary.Fail != 0 {
		t.Fatalf("worker-offline readiness failed doctor: %+v", report)
	}
	for _, check := range report.Checks {
		if check.ID == "service_health" {
			if check.Status != doctorWarn || !strings.Contains(check.Message, "no Human worker is online") {
				t.Fatalf("worker-offline gateway check = %+v", check)
			}
			return
		}
	}
	t.Fatal("doctor omitted service_health check")
}

func TestProbeHealthRejectsLegacyFixedOKWithoutReadinessEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	got := probeHealth(context.Background(), server.URL+"/healthz")
	if !got.Running || got.Err == nil {
		t.Fatalf("legacy fixed-ok probe = %+v", got)
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
