package humancmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/userdata"
	localruntime "github.com/vibe-agi/human/local"
)

const localCredentialVersion = 1

type localCredentialFile struct {
	Version            int                  `json:"version"`
	Active             *localCredentialPair `json:"active,omitempty"`
	Rotation           *localCredentialPair `json:"rotation,omitempty"`
	PendingRevocations []localCredential    `json:"pending_revocations,omitempty"`
}

type localCredentialPair struct {
	Caller localCredential `json:"caller"`
	Worker localCredential `json:"worker"`
}

type localCredential struct {
	Type      gateway.PrincipalType `json:"type"`
	SubjectID string                `json:"subject_id"`
	KeyID     string                `json:"key_id"`
	Secret    string                `json:"secret"`
	Revoking  bool                  `json:"revoking,omitempty"`
}

type localCredentialAuthority interface {
	PrepareToken(gateway.PrincipalType, string) (gateway.PreparedToken, error)
	ActivateToken(context.Context, gateway.PreparedToken) error
	ValidateToken(context.Context, string) (gateway.Principal, error)
	RevokeToken(context.Context, string, string) error
}

func newLocalCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "local", Short: "run gateway, SQLite, and the browser human side in one process",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runLocal(command.Context(), command.OutOrStdout(), settings)
		},
	}
	flags := command.Flags()
	flags.String("listen", "127.0.0.1:19080", "loopback model API listen address")
	flags.Bool("workspace-auto-send", false, "send clean mirror changes after live detection and fresh review")
	flags.Int("queue-capacity", 32, "maximum admitted and active requests")
	flags.Duration("stream-write-timeout", 10*time.Second, "maximum time for one SSE write and flush")
	flags.Duration("max-pending", 10*time.Minute, "maximum time waiting for a Human response")
	flags.Duration("shutdown-timeout", 10*time.Second, "graceful local shutdown timeout")
	flags.Bool("reset-credentials", false, "rotate the persisted local caller and worker credentials")
	flags.String("web", "", "loopback address of the browser human side (default 127.0.0.1:19081)")
	flags.Bool("no-auto-title", false, "do not auto-answer tool-less chat requests with a derived title; route them to the human inbox instead")
	persistent := command.PersistentFlags()
	persistent.String("workspace", ".", "workspace used to isolate local private state; nearest Git root is selected")
	persistent.String("db", automaticPrivatePath, "embedded SQLite database; auto uses OS user data isolated by workspace")
	persistent.String("credentials", automaticPrivatePath, "mode-0600 credential file; auto uses OS user data isolated by workspace")
	persistent.String("mirror-root", "~/mirror", "worker-local workspace mirror root")
	persistent.String("outbox", automaticPrivatePath, "private SQLite outbox; auto uses OS user data isolated by workspace")
	persistent.String("state-db", automaticPrivatePath, "private SQLite TUI recovery state; auto uses OS user data, empty disables persistence")
	persistent.String("caller-subject", "local-caller", "stable local caller identity")
	persistent.String("worker-subject", "local-worker", "stable local worker identity")

	for key, name := range map[string]string{
		"local.listen": "listen", "local.workspace_auto_send": "workspace-auto-send",
		"local.queue_capacity":       "queue-capacity",
		"local.stream_write_timeout": "stream-write-timeout",
		"local.max_pending":          "max-pending", "local.shutdown_timeout": "shutdown-timeout",
		"local.reset_credentials": "reset-credentials", "local.web": "web",
		"local.no_auto_title": "no-auto-title",
	} {
		mustBind(settings, key, flags.Lookup(name))
	}
	for key, name := range map[string]string{
		"local.workspace": "workspace", "local.db": "db", "local.credentials": "credentials",
		"local.mirror_root": "mirror-root", "local.outbox": "outbox", "local.state_db": "state-db",
		"local.caller_subject": "caller-subject", "local.worker_subject": "worker-subject",
	} {
		mustBind(settings, key, persistent.Lookup(name))
	}
	credentialsCommand := &cobra.Command{
		Use: "credentials", Short: "print the local caller credential as JSON",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			workspaceRoot, err := resolveLocalWorkspace(settings.GetString("local.workspace"))
			if err != nil {
				return err
			}
			path, err := resolveWorkspaceDataPath(
				settings.GetString("local.credentials"), "local credential file", "local", workspaceRoot, "credentials.json",
			)
			if err != nil {
				return err
			}
			credentials, found, err := readLocalCredentials(path)
			if err != nil {
				return err
			}
			if !found {
				return errors.New("local credentials do not exist; start `human local` first")
			}
			if credentials.Active == nil {
				return errors.New("local credential rotation is incomplete; start `human local` to recover it")
			}
			if tokenOnly, _ := command.Flags().GetBool("token-only"); tokenOnly {
				_, err := fmt.Fprintln(command.OutOrStdout(), credentials.Active.Caller.Secret)
				return err
			}
			return json.NewEncoder(command.OutOrStdout()).Encode(map[string]string{
				"caller_token": credentials.Active.Caller.Secret,
			})
		},
	}
	credentialsCommand.Flags().Bool("token-only", false, "print only the caller token for shell command substitution")
	command.AddCommand(credentialsCommand)
	command.AddCommand(newLocalBackupCommand(settings))
	command.AddCommand(newLocalVerifyBackupCommand())
	command.AddCommand(newLocalRestoreCommand(settings))
	return command
}

func runLocal(ctx context.Context, output io.Writer, settings *viper.Viper) error {
	workspaceRoot, err := resolveLocalWorkspace(settings.GetString("local.workspace"))
	if err != nil {
		return err
	}
	databasePath, err := resolveWorkspaceDataPath(
		settings.GetString("local.db"), "local database", "local", workspaceRoot, "gateway.db",
	)
	if err != nil {
		return err
	}
	if err := rejectPendingLocalRestore(databasePath); err != nil {
		return err
	}
	credentialPath, err := resolveWorkspaceDataPath(
		settings.GetString("local.credentials"), "local credential file", "local", workspaceRoot, "credentials.json",
	)
	if err != nil {
		return err
	}
	mirrorRoot, err := resolveMirrorRoot(settings.GetString("local.mirror_root"))
	if err != nil {
		return err
	}
	outboxPath, err := resolveWorkspaceDataPath(
		settings.GetString("local.outbox"), "local worker outbox", "local", workspaceRoot, "worker-outbox.db",
	)
	if err != nil {
		return err
	}
	statePath := ""
	if strings.TrimSpace(settings.GetString("local.state_db")) != "" {
		statePath, err = resolveWorkspaceDataPath(
			settings.GetString("local.state_db"), "local worker state database", "local", workspaceRoot, "worker-state.db",
		)
	}
	if err != nil {
		return err
	}

	credentialState, found, err := readLocalCredentials(credentialPath)
	if err != nil {
		return err
	}
	reset := settings.GetBool("local.reset_credentials")
	config := localruntime.Config{
		Gateway: gateway.Config{
			DatabasePath:       databasePath,
			QueueCapacity:      settings.GetInt("local.queue_capacity"),
			StreamWriteTimeout: settings.GetDuration("local.stream_write_timeout"),
			MaxPending:         settings.GetDuration("local.max_pending"),
		},
		Worker: localruntime.WorkerPaths{
			MirrorRoot: mirrorRoot, OutboxPath: outboxPath,
		},
		ListenAddress:       settings.GetString("local.listen"),
		CallerSubject:       settings.GetString("local.caller_subject"),
		WorkerSubject:       settings.GetString("local.worker_subject"),
		ShutdownTimeout:     settings.GetDuration("local.shutdown_timeout"),
		WebListenAddress:    settings.GetString("local.web"),
		WebDisableAutoTitle: settings.GetBool("local.no_auto_title"),
	}
	if strings.TrimSpace(config.WebListenAddress) == "" {
		config.WebListenAddress = "127.0.0.1:19081"
	}
	_ = statePath

	// Rotation runs against the same recovered gateway Local will serve, before
	// its HTTP listener or worker starts. A prepared pair is first fsynced into
	// the private journal, then activated, then made active atomically. Thus a
	// crash always leaves either the old or new pair recoverable without opening
	// the completion gateway twice during startup.
	config.CredentialProvider = func(providerContext context.Context, server *gateway.Server) (localruntime.Credentials, error) {
		var providerErr error
		credentialState, providerErr = reconcileLocalCredentialState(
			providerContext,
			server,
			func(state localCredentialFile) error { return writeLocalCredentials(credentialPath, state) },
			credentialState,
			found,
			reset,
			config.CallerSubject,
			config.WorkerSubject,
		)
		if providerErr != nil {
			return localruntime.Credentials{}, providerErr
		}
		active := credentialState.Active
		return localruntime.Credentials{
			CallerToken: active.Caller.Secret, WorkerToken: active.Worker.Secret,
			CallerKeyID: active.Caller.KeyID, WorkerKeyID: active.Worker.KeyID,
		}, nil
	}

	instance, err := localruntime.Open(ctx, config)
	if err != nil {
		return fmt.Errorf("open local with persisted credentials: %w", err)
	}
	defer instance.Close()

	fmt.Fprintf(output, "Human local is ready\nworkspace scope: %s\nmodel base URL: %s/v1\ncaller credential: %s\n", workspaceRoot, instance.BaseURL(), credentialPath)
	fmt.Fprintln(output, "in the same workspace: export HUMAN_CALLER_TOKEN=\"$(human local credentials --workspace . --token-only)\"")
	fmt.Fprintf(output, "human side (browser): %s\n", instance.WebURL())
	waitDone := make(chan error, 1)
	go func() { waitDone <- instance.Wait() }()
	select {
	case err := <-waitDone:
		return err
	case <-ctx.Done():
		return nil
	}
}

func resolveLocalWorkspace(value string) (string, error) {
	absolute, err := resolvePrivatePath(value, "local workspace")
	if err != nil {
		return "", err
	}
	workspace, err := userdata.ResolveGitWorkspace(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve local workspace: %w", err)
	}
	return workspace, nil
}

func readLocalCredentials(path string) (localCredentialFile, bool, error) {
	const maxCredentialBytes = 64 << 10
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return localCredentialFile{}, false, nil
	}
	if err != nil {
		return localCredentialFile{}, false, fmt.Errorf("inspect local credentials: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return localCredentialFile{}, false, errors.New("local credential path is not a regular file")
	}
	if info.Size() > maxCredentialBytes {
		return localCredentialFile{}, false, errors.New("local credential file exceeds the 64 KiB limit")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return localCredentialFile{}, false, fmt.Errorf("local credential file %s must not be accessible by group or others", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return localCredentialFile{}, false, fmt.Errorf("open local credentials: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return localCredentialFile{}, false, fmt.Errorf("inspect opened local credentials: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return localCredentialFile{}, false, errors.New("local credential file changed while opening")
	}
	if runtime.GOOS != "windows" && opened.Mode().Perm()&0o077 != 0 {
		return localCredentialFile{}, false, fmt.Errorf("local credential file %s must not be accessible by group or others", path)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxCredentialBytes+1))
	if err != nil {
		return localCredentialFile{}, false, fmt.Errorf("read local credentials: %w", err)
	}
	if len(payload) > maxCredentialBytes {
		return localCredentialFile{}, false, errors.New("local credential file exceeds the 64 KiB limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var credentials localCredentialFile
	if err := decoder.Decode(&credentials); err != nil {
		return localCredentialFile{}, false, fmt.Errorf("decode local credentials: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return localCredentialFile{}, false, fmt.Errorf("decode local credentials: trailing data: %w", err)
	}
	if err := validateLocalCredentialState(credentials); err != nil {
		return localCredentialFile{}, false, err
	}
	return credentials, true, nil
}

func writeLocalCredentials(path string, credentials localCredentialFile) error {
	if err := validateLocalCredentialState(credentials); err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	if info, err := os.Stat(directory); err != nil {
		return err
	} else if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("local credential directory %s must not be accessible by group or others", directory)
	}
	temporary, err := os.CreateTemp(directory, ".local-credentials-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(credentials); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		directoryHandle, err := os.Open(directory)
		if err != nil {
			return err
		}
		syncErr := directoryHandle.Sync()
		closeErr := directoryHandle.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func reconcileLocalCredentialState(
	ctx context.Context,
	authority localCredentialAuthority,
	write func(localCredentialFile) error,
	state localCredentialFile,
	found, rotate bool,
	callerSubject, workerSubject string,
) (localCredentialFile, error) {
	if authority == nil || write == nil {
		return localCredentialFile{}, errors.New("local credential authority and journal writer are required")
	}
	if !found {
		state = localCredentialFile{Version: localCredentialVersion}
	} else if err := validateLocalCredentialState(state); err != nil {
		return localCredentialFile{}, err
	}
	recoveringRotation := state.Rotation != nil || len(state.PendingRevocations) > 0

	var err error
	state, err = finishLocalCredentialRotation(ctx, authority, write, state)
	if err != nil {
		return state, err
	}
	if state.Active != nil {
		if err := validateLocalCredentialPair(ctx, authority, *state.Active); err != nil {
			return state, fmt.Errorf("validate active local credentials: %w", err)
		}
	}
	state, err = drainLocalCredentialRevocations(ctx, authority, write, state)
	if err != nil {
		return state, err
	}

	// Persisted preparation or revocation work means this invocation is
	// recovering the requested reset. Do not rotate a second time merely because
	// the user reran the same --reset-credentials command after a crash.
	if state.Active == nil || (rotate && !recoveringRotation) {
		pair, err := prepareLocalCredentialPair(authority, callerSubject, workerSubject)
		if err != nil {
			return state, err
		}
		state.Rotation = &pair
		if err := write(state); err != nil {
			return state, fmt.Errorf("persist prepared local credentials: %w", err)
		}
		state, err = finishLocalCredentialRotation(ctx, authority, write, state)
		if err != nil {
			return state, err
		}
		state, err = drainLocalCredentialRevocations(ctx, authority, write, state)
		if err != nil {
			return state, err
		}
	}

	if state.Active == nil {
		return state, errors.New("local credential reconciliation produced no active pair")
	}
	if state.Active.Caller.SubjectID != callerSubject || state.Active.Worker.SubjectID != workerSubject {
		return state, fmt.Errorf(
			"persisted local credential subjects (%q, %q) do not match configured subjects (%q, %q); rotate credentials intentionally",
			state.Active.Caller.SubjectID, state.Active.Worker.SubjectID, callerSubject, workerSubject,
		)
	}
	return state, nil
}

func finishLocalCredentialRotation(
	ctx context.Context,
	authority localCredentialAuthority,
	write func(localCredentialFile) error,
	state localCredentialFile,
) (localCredentialFile, error) {
	if state.Rotation == nil {
		return state, nil
	}
	rotation := *state.Rotation
	for _, credential := range []localCredential{rotation.Caller, rotation.Worker} {
		if err := authority.ActivateToken(ctx, credential.prepared()); err != nil {
			return state, fmt.Errorf("activate prepared %s credential: %w", credential.Type, err)
		}
	}
	if state.Active != nil {
		state.PendingRevocations = append(
			state.PendingRevocations,
			state.Active.Worker,
			state.Active.Caller,
		)
	}
	state.Active = &rotation
	state.Rotation = nil
	if err := write(state); err != nil {
		return state, fmt.Errorf("commit active local credentials: %w", err)
	}
	return state, nil
}

func drainLocalCredentialRevocations(
	ctx context.Context,
	authority localCredentialAuthority,
	write func(localCredentialFile) error,
	state localCredentialFile,
) (localCredentialFile, error) {
	for len(state.PendingRevocations) > 0 {
		pending := state.PendingRevocations[0]
		principal, err := authority.ValidateToken(ctx, pending.Secret)
		if err != nil {
			if !errors.Is(err, gateway.ErrUnauthorized) || !pending.Revoking {
				return state, fmt.Errorf("validate pending %s credential revocation: %w", pending.Type, err)
			}
			// Revocation committed before the previous process could remove the
			// journal entry. The persisted Revoking marker makes this unambiguous.
		} else {
			if err := credentialMatchesPrincipal(pending, principal); err != nil {
				return state, fmt.Errorf("validate pending %s credential revocation: %w", pending.Type, err)
			}
			if !pending.Revoking {
				state.PendingRevocations[0].Revoking = true
				pending.Revoking = true
				if err := write(state); err != nil {
					return state, fmt.Errorf("mark local credential revocation: %w", err)
				}
			}
			if err := authority.RevokeToken(ctx, pending.Secret, pending.KeyID); err != nil &&
				!errors.Is(err, gateway.ErrUnauthorized) {
				return state, fmt.Errorf("revoke previous %s credential: %w", pending.Type, err)
			}
		}
		state.PendingRevocations = append([]localCredential(nil), state.PendingRevocations[1:]...)
		if err := write(state); err != nil {
			return state, fmt.Errorf("complete local credential revocation: %w", err)
		}
	}
	return state, nil
}

func prepareLocalCredentialPair(
	authority localCredentialAuthority,
	callerSubject, workerSubject string,
) (localCredentialPair, error) {
	caller, err := authority.PrepareToken(gateway.PrincipalCaller, callerSubject)
	if err != nil {
		return localCredentialPair{}, fmt.Errorf("prepare local caller credential: %w", err)
	}
	worker, err := authority.PrepareToken(gateway.PrincipalWorker, workerSubject)
	if err != nil {
		return localCredentialPair{}, fmt.Errorf("prepare local worker credential: %w", err)
	}
	return localCredentialPair{
		Caller: localCredentialFromPrepared(caller),
		Worker: localCredentialFromPrepared(worker),
	}, nil
}

func validateLocalCredentialPair(
	ctx context.Context,
	authority localCredentialAuthority,
	pair localCredentialPair,
) error {
	for _, credential := range []localCredential{pair.Caller, pair.Worker} {
		principal, err := authority.ValidateToken(ctx, credential.Secret)
		if err != nil {
			return fmt.Errorf("%s token: %w", credential.Type, err)
		}
		if err := credentialMatchesPrincipal(credential, principal); err != nil {
			return err
		}
	}
	return nil
}

func credentialMatchesPrincipal(credential localCredential, principal gateway.Principal) error {
	if principal.Type != credential.Type || principal.SubjectID != credential.SubjectID || principal.KeyID != credential.KeyID {
		return gateway.ErrCredentialMismatch
	}
	return nil
}

func localCredentialFromPrepared(prepared gateway.PreparedToken) localCredential {
	return localCredential{
		Type: prepared.Type, SubjectID: prepared.SubjectID,
		KeyID: prepared.KeyID, Secret: prepared.Secret,
	}
}

func (credential localCredential) prepared() gateway.PreparedToken {
	return gateway.PreparedToken{
		Type: credential.Type, SubjectID: credential.SubjectID,
		KeyID: credential.KeyID, Secret: credential.Secret,
	}
}

func validateLocalCredentialState(state localCredentialFile) error {
	if state.Version != localCredentialVersion {
		return errors.New("local credential file has an unsupported version")
	}
	if state.Active == nil && state.Rotation == nil {
		return errors.New("local credential file has neither an active nor prepared credential pair")
	}
	if state.Active != nil {
		if err := validateLocalCredentialPairShape(*state.Active, false); err != nil {
			return fmt.Errorf("invalid active local credentials: %w", err)
		}
	}
	if state.Rotation != nil {
		if err := validateLocalCredentialPairShape(*state.Rotation, false); err != nil {
			return fmt.Errorf("invalid prepared local credentials: %w", err)
		}
	}
	seen := make(map[string]struct{})
	if state.Active != nil {
		seen[state.Active.Caller.KeyID] = struct{}{}
		seen[state.Active.Worker.KeyID] = struct{}{}
	}
	if state.Rotation != nil {
		for _, credential := range []localCredential{state.Rotation.Caller, state.Rotation.Worker} {
			if _, duplicate := seen[credential.KeyID]; duplicate {
				return errors.New("local credential file contains a duplicate key ID")
			}
			seen[credential.KeyID] = struct{}{}
		}
	}
	for _, pending := range state.PendingRevocations {
		if err := validateLocalCredentialShape(pending, true); err != nil {
			return fmt.Errorf("invalid pending local credential revocation: %w", err)
		}
		if _, duplicate := seen[pending.KeyID]; duplicate {
			return errors.New("local credential file contains a duplicate key ID")
		}
		seen[pending.KeyID] = struct{}{}
	}
	return nil
}

func validateLocalCredentialPairShape(pair localCredentialPair, allowRevoking bool) error {
	if pair.Caller.Type != gateway.PrincipalCaller || pair.Worker.Type != gateway.PrincipalWorker {
		return errors.New("credential pair has incorrect principal types")
	}
	if err := validateLocalCredentialShape(pair.Caller, allowRevoking); err != nil {
		return err
	}
	return validateLocalCredentialShape(pair.Worker, allowRevoking)
}

func validateLocalCredentialShape(credential localCredential, allowRevoking bool) error {
	if credential.Type != gateway.PrincipalCaller && credential.Type != gateway.PrincipalWorker {
		return errors.New("credential principal type must be caller or worker")
	}
	if strings.TrimSpace(credential.SubjectID) == "" || strings.TrimSpace(credential.KeyID) == "" ||
		strings.TrimSpace(credential.Secret) == "" {
		return errors.New("credential is incomplete")
	}
	if credential.Revoking && !allowRevoking {
		return errors.New("active or prepared credential is marked revoking")
	}
	return nil
}
