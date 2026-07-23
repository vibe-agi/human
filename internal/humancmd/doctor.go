package humancmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/userdata"
)

const defaultDoctorHealthURL = "http://127.0.0.1:19080/readyz"

type doctorStatus string

const (
	doctorPass doctorStatus = "pass"
	doctorWarn doctorStatus = "warn"
	doctorFail doctorStatus = "fail"
)

type doctorCheck struct {
	ID      string       `json:"id"`
	Status  doctorStatus `json:"status"`
	Message string       `json:"message"`
}

type doctorSummary struct {
	Pass int `json:"pass"`
	Warn int `json:"warn"`
	Fail int `json:"fail"`
}

type doctorReport struct {
	SchemaVersion int           `json:"schema_version"`
	OK            bool          `json:"ok"`
	Workspace     string        `json:"workspace"`
	DataRoot      string        `json:"data_root"`
	WorkspaceData string        `json:"workspace_data"`
	Checks        []doctorCheck `json:"checks"`
	Summary       doctorSummary `json:"summary"`
}

type doctorOpenCodeResult struct {
	Found   bool
	Path    string
	Version string
	Err     error
}

type doctorHealthResult struct {
	Running          bool
	Observed         bool
	DatabaseStatus   string
	RecoveryComplete bool
	OnlineWorkers    int
	HasOnlineWorker  bool
	Err              error
}

type boundedDoctorOutput struct {
	mu   sync.Mutex
	data []byte
}

func (output *boundedDoctorOutput) Write(payload []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	const limit = 64 << 10
	if remaining := limit - len(output.data); remaining > 0 {
		output.data = append(output.data, payload[:min(len(payload), remaining)]...)
	}
	// Report the complete write even after the diagnostic prefix is full. The
	// child cannot turn excessive version output into an unbounded allocation or
	// an artificial broken-pipe failure.
	return len(payload), nil
}

func (output *boundedDoctorOutput) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return string(output.data)
}

type doctorDependencies struct {
	openCode func(context.Context) doctorOpenCodeResult
	health   func(context.Context, string) doctorHealthResult
}

func newDoctorCommand() *cobra.Command {
	return newDoctorCommandWithDependencies(doctorDependencies{
		openCode: probeOpenCode,
		health:   probeHealth,
	})
}

func newDoctorCommandWithDependencies(dependencies doctorDependencies) *cobra.Command {
	var workspace string
	var healthURL string
	var outputJSON bool
	var requireOpenCode bool
	command := &cobra.Command{
		Use:   "doctor",
		Short: "check local Human and OpenCode readiness",
		Args:  cobra.NoArgs,
		// Diagnostics must still run when an unrelated optional config file is
		// malformed; every setting this command uses is explicit below.
		PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
		RunE: func(command *cobra.Command, _ []string) error {
			report := runDoctor(command.Context(), workspace, healthURL, requireOpenCode, dependencies)
			var err error
			if outputJSON {
				encoder := json.NewEncoder(command.OutOrStdout())
				encoder.SetEscapeHTML(false)
				err = encoder.Encode(report)
			} else {
				err = writeDoctorHuman(command.OutOrStdout(), report)
			}
			if err != nil {
				return err
			}
			if report.Summary.Fail != 0 {
				return fmt.Errorf("doctor found %d failed check(s)", report.Summary.Fail)
			}
			return nil
		},
	}
	command.Flags().StringVar(&workspace, "workspace", ".", "workspace to diagnose; the nearest Git root is selected")
	command.Flags().StringVar(&healthURL, "health-url", defaultDoctorHealthURL, "Human service readiness endpoint")
	command.Flags().BoolVar(&requireOpenCode, "require-opencode", false, "fail unless the exact supported OpenCode Workspace adapter is installed")
	command.Flags().BoolVar(&outputJSON, "json", false, "print stable machine-readable JSON")
	return command
}

func runDoctor(ctx context.Context, workspaceValue, healthURL string, requireOpenCode bool, dependencies doctorDependencies) doctorReport {
	report := doctorReport{SchemaVersion: 1, Checks: make([]doctorCheck, 0, 5)}
	add := func(id string, status doctorStatus, message string) {
		report.Checks = append(report.Checks, doctorCheck{ID: id, Status: status, Message: message})
		switch status {
		case doctorPass:
			report.Summary.Pass++
		case doctorWarn:
			report.Summary.Warn++
		case doctorFail:
			report.Summary.Fail++
		}
	}

	workspace, workspaceErr := userdata.ResolveGitWorkspace(workspaceValue)
	if workspaceErr != nil {
		add("workspace", doctorFail, workspaceErr.Error())
	} else {
		report.Workspace = workspace
		add("workspace", doctorPass, "real workspace and nearest Git root resolved")
	}

	dataRoot, dataRootErr := userdata.Root()
	if dataRootErr != nil {
		add("private_data", doctorFail, dataRootErr.Error())
	} else if workspaceErr != nil {
		report.DataRoot = dataRoot
		add("private_data", doctorFail, "workspace-scoped private data cannot be resolved until the workspace is valid")
	} else {
		report.DataRoot = dataRoot
		workspaceData, err := userdata.WorkspacePath("local", workspace)
		if err != nil {
			add("private_data", doctorFail, err.Error())
		} else {
			report.WorkspaceData = workspaceData
			if err := probePrivateDirectory(dataRoot, workspaceData); err != nil {
				add("private_data", doctorFail, err.Error())
			} else {
				add("private_data", doctorPass, "private user-data directory is writable and permission-restricted")
			}
		}
	}

	if report.WorkspaceData == "" {
		add("local_credentials", doctorWarn, "local credentials were not checked because private workspace data is unavailable")
	} else {
		credentialPath := filepath.Join(report.WorkspaceData, "credentials.json")
		_, found, err := readPublicCredentials(credentialPath)
		switch {
		case err != nil:
			add("local_credentials", doctorFail, err.Error())
		case !found:
			add("local_credentials", doctorWarn, "local credentials do not exist yet; start `human local --workspace .` once")
		default:
			add("local_credentials", doctorPass, "local caller credential is private and valid")
		}
	}

	if dependencies.openCode == nil {
		add("opencode", doctorFail, "OpenCode diagnostic is unavailable")
	} else {
		result := dependencies.openCode(ctx)
		compatibilityStatus := doctorWarn
		if requireOpenCode {
			compatibilityStatus = doctorFail
		}
		switch {
		case !result.Found:
			add("opencode", compatibilityStatus, fmt.Sprintf("OpenCode is not present in PATH; the exact Workspace profile requires %s", adapter.OpenCodeVersion))
		case result.Err != nil:
			add("opencode", compatibilityStatus, fmt.Sprintf("OpenCode at %s could not report its version; the exact Workspace profile requires %s: %v", result.Path, adapter.OpenCodeVersion, result.Err))
		case result.Version != adapter.OpenCodeVersion:
			add("opencode", compatibilityStatus, fmt.Sprintf("OpenCode %s does not match the exact Workspace profile; it requires %s", result.Version, adapter.OpenCodeVersion))
		default:
			add("opencode", doctorPass, fmt.Sprintf("OpenCode %s is the exact supported adapter version", result.Version))
		}
	}

	if dependencies.health == nil {
		add("service_health", doctorFail, "service health diagnostic is unavailable")
	} else {
		result := dependencies.health(ctx, healthURL)
		switch {
		case !result.Running:
			message := "Human service is not running; this is expected before `human local` starts"
			if result.Err != nil {
				message += ": " + result.Err.Error()
			}
			add("service_health", doctorWarn, message)
		case result.Err != nil:
			add("service_health", doctorFail, result.Err.Error())
		case result.Observed && !result.HasOnlineWorker:
			add("service_health", doctorWarn, "Human service is ready (SQLite ok; recovery complete), but no Human worker is online")
		case result.Observed:
			add("service_health", doctorPass, fmt.Sprintf(
				"Human service is ready (SQLite ok; recovery complete); %d Human worker(s) online",
				result.OnlineWorkers,
			))
		default:
			add("service_health", doctorPass, "Human service readiness endpoint returned status ok")
		}
	}

	report.OK = report.Summary.Fail == 0
	return report
}

func probePrivateDirectory(dataRoot, workspaceData string) error {
	relative, err := filepath.Rel(dataRoot, workspaceData)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("workspace user-data path escapes the Human data root")
	}
	if err := os.MkdirAll(workspaceData, 0o700); err != nil {
		return fmt.Errorf("create private user-data directory: %w", err)
	}
	current := dataRoot
	paths := []string{current}
	if relative != "." {
		for _, part := range strings.Split(relative, string(filepath.Separator)) {
			current = filepath.Join(current, part)
			paths = append(paths, current)
		}
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("inspect private user-data directory %s: %w", path, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("private user-data path %s is not a directory", path)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("private user-data directory %s must not be accessible by group or others", path)
		}
	}
	probe, err := os.CreateTemp(workspaceData, ".human-doctor-*")
	if err != nil {
		return fmt.Errorf("create private user-data probe: %w", err)
	}
	probePath := probe.Name()
	defer os.Remove(probePath)
	if err := probe.Chmod(0o600); err != nil {
		_ = probe.Close()
		return fmt.Errorf("restrict private user-data probe: %w", err)
	}
	if _, err := io.WriteString(probe, "human-doctor\n"); err != nil {
		_ = probe.Close()
		return fmt.Errorf("write private user-data probe: %w", err)
	}
	if err := probe.Sync(); err != nil {
		_ = probe.Close()
		return fmt.Errorf("sync private user-data probe: %w", err)
	}
	if err := probe.Close(); err != nil {
		return fmt.Errorf("close private user-data probe: %w", err)
	}
	if err := os.Remove(probePath); err != nil {
		return fmt.Errorf("remove private user-data probe: %w", err)
	}
	return nil
}

func probeOpenCode(ctx context.Context) doctorOpenCodeResult {
	path, err := exec.LookPath("opencode")
	if err != nil {
		return doctorOpenCodeResult{Found: false}
	}
	versionContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var output boundedDoctorOutput
	command := exec.CommandContext(versionContext, path, "--version")
	command.Stdout = &output
	command.Stderr = &output
	command.WaitDelay = time.Second
	err = command.Run()
	return doctorOpenCodeResult{
		Found: true, Path: path, Version: strings.TrimSpace(output.String()), Err: err,
	}
}

func probeHealth(ctx context.Context, healthURL string) doctorHealthResult {
	healthURL = strings.TrimSpace(healthURL)
	parsed, err := url.Parse(healthURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return doctorHealthResult{Running: true, Err: errors.New("gateway health URL must be an absolute HTTP(S) URL")}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return doctorHealthResult{Running: true, Err: errors.New("gateway health URL must use http or https")}
	}
	if parsed.User != nil {
		return doctorHealthResult{Running: true, Err: errors.New("gateway health URL must not contain credentials")}
	}
	requestContext, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, healthURL, nil)
	if err != nil {
		return doctorHealthResult{Running: true, Err: fmt.Errorf("invalid gateway health URL: %w", err)}
	}
	response, err := (&http.Client{Timeout: 750 * time.Millisecond}).Do(request)
	if err != nil {
		return doctorHealthResult{Running: false, Err: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return doctorHealthResult{Running: true, Err: fmt.Errorf("gateway health returned HTTP %d", response.StatusCode)}
	}
	var payload struct {
		Status   string `json:"status"`
		Database struct {
			Status string `json:"status"`
		} `json:"database"`
		Recovery struct {
			Complete *bool `json:"complete"`
		} `json:"recovery"`
		Workers struct {
			Online    *int  `json:"online"`
			HasOnline *bool `json:"has_online"`
		} `json:"workers"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4<<10))
	if err := decoder.Decode(&payload); err != nil {
		return doctorHealthResult{Running: true, Err: fmt.Errorf("decode gateway health response: %w", err)}
	}
	if payload.Status != "ok" {
		return doctorHealthResult{Running: true, Err: fmt.Errorf("gateway health status is %q, want ok", payload.Status)}
	}
	if payload.Database.Status != "ok" {
		return doctorHealthResult{Running: true, Err: fmt.Errorf("gateway database status is %q, want ok", payload.Database.Status)}
	}
	if payload.Recovery.Complete == nil || !*payload.Recovery.Complete {
		return doctorHealthResult{Running: true, Err: errors.New("gateway recovery is not complete")}
	}
	if payload.Workers.Online == nil || *payload.Workers.Online < 0 || payload.Workers.HasOnline == nil {
		return doctorHealthResult{Running: true, Err: errors.New("gateway readiness response has invalid worker status")}
	}
	hasOnline := *payload.Workers.Online > 0
	if *payload.Workers.HasOnline != hasOnline {
		return doctorHealthResult{Running: true, Err: errors.New("gateway readiness response has inconsistent worker status")}
	}
	return doctorHealthResult{
		Running: true, Observed: true, DatabaseStatus: payload.Database.Status,
		RecoveryComplete: true, OnlineWorkers: *payload.Workers.Online,
		HasOnlineWorker: hasOnline,
	}
}

func writeDoctorHuman(writer io.Writer, report doctorReport) error {
	if _, err := fmt.Fprintf(writer, "Human doctor\nworkspace: %s\ndata root: %s\nworkspace data: %s\n", report.Workspace, report.DataRoot, report.WorkspaceData); err != nil {
		return err
	}
	for _, check := range report.Checks {
		if _, err := fmt.Fprintf(writer, "%-4s  %-18s %s\n", strings.ToUpper(string(check.Status)), check.ID, check.Message); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(writer, "summary: %d pass, %d warn, %d fail\n", report.Summary.Pass, report.Summary.Warn, report.Summary.Fail)
	return err
}
