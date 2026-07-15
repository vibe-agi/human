package humancmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/delegation"
	delegationworker "github.com/vibe-agi/human/internal/delegation/worker"
	delegationclient "github.com/vibe-agi/human/internal/delegation/workerclient"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
	"github.com/vibe-agi/human/internal/worktree"
)

const defaultDelegationGateway = "ws://127.0.0.1:8080/internal/v1/delegation/worker/ws"

func newDelegationCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use:     "delegation",
		Aliases: []string{"async"},
		Short:   "work asynchronous delegation tasks",
		Args:    cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	flags := command.PersistentFlags()
	flags.String("gateway", defaultDelegationGateway, "humand delegation worker WebSocket URL")
	flags.String("token", "", "worker token")
	flags.String("outbox", "~/.human/delegation-worker-outbox.db", "private SQLite command outbox")
	flags.String("repository", ".", "worker-local git repository")
	flags.String("worktree-root", "", "task worktree root; defaults to <repository>/.human/wt")
	flags.String("shared-remote", "", "configured git remote name used to fetch immutable task bases")
	flags.StringArray("setup-arg", nil, "setup hook argv; repeat for each argument, first value is the executable")
	flags.Int64("max-file-bytes", 16<<20, "maximum size of each delivered file")
	flags.String("author-name", "", "git author name for automatic turn commits")
	flags.String("author-email", "", "git author email for automatic turn commits")
	flags.Duration("timeout", 30*time.Second, "connection and task operation timeout")
	mustBind(settings, "delegation.gateway", flags.Lookup("gateway"))
	mustBind(settings, "delegation.token", flags.Lookup("token"))
	mustBind(settings, "delegation.outbox", flags.Lookup("outbox"))
	mustBind(settings, "delegation.repository", flags.Lookup("repository"))
	mustBind(settings, "delegation.worktree_root", flags.Lookup("worktree-root"))
	mustBind(settings, "delegation.shared_remote", flags.Lookup("shared-remote"))
	mustBind(settings, "delegation.setup_args", flags.Lookup("setup-arg"))
	mustBind(settings, "delegation.max_file_bytes", flags.Lookup("max-file-bytes"))
	mustBind(settings, "delegation.author_name", flags.Lookup("author-name"))
	mustBind(settings, "delegation.author_email", flags.Lookup("author-email"))
	mustBind(settings, "delegation.timeout", flags.Lookup("timeout"))

	command.AddCommand(newDelegationWatchCommand(settings))
	command.AddCommand(newDelegationTasksCommand(settings))
	command.AddCommand(newDelegationAcceptCommand(settings))
	command.AddCommand(newDelegationDeliverCommand(settings))
	command.AddCommand(newDelegationExecCommand(settings))
	command.AddCommand(newDelegationRejectCommand(settings))
	command.AddCommand(newDelegationTransitionCommand(settings, "complete"))
	command.AddCommand(newDelegationTransitionCommand(settings, "fail"))
	command.AddCommand(newDelegationRewindCommand(settings))
	return command
}

func newDelegationWatchCommand(settings *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use: "watch", Short: "register this worker and stream task snapshots as JSON lines",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(command.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			client, err := connectDelegationClient(ctx, settings)
			if err != nil {
				return err
			}
			defer client.Close()
			encoder := json.NewEncoder(command.OutOrStdout())
			if err := encoder.Encode(map[string]any{
				"type": "registered", "worker_id": client.WorkerID(),
			}); err != nil {
				return err
			}
			for {
				select {
				case <-ctx.Done():
					return nil
				case update, open := <-client.Updates():
					if !open {
						return nil
					}
					if err := encodeDelegationUpdate(encoder, update); err != nil {
						return err
					}
				}
			}
		},
	}
}

func newDelegationTasksCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "tasks", Short: "show the recoverable submitted and owned task queue",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, stop := operationContext(command.Context(), settings)
			defer stop()
			client, err := connectDelegationClient(ctx, settings)
			if err != nil {
				return err
			}
			defer client.Close()
			settle := settings.GetDuration("delegation.tasks.settle")
			if settle <= 0 {
				return errors.New("task recovery settle duration must be positive")
			}
			tasks, err := collectRecoveredTasks(ctx, client, settle)
			if err != nil {
				return err
			}
			return writeJSON(command.OutOrStdout(), struct {
				WorkerID string               `json:"worker_id"`
				Tasks    []delegationTaskView `json:"tasks"`
			}{WorkerID: client.WorkerID(), Tasks: tasks})
		},
	}
	command.Flags().Duration("settle", 500*time.Millisecond, "quiet period used to finish the recovery snapshot")
	mustBind(settings, "delegation.tasks.settle", command.Flags().Lookup("settle"))
	return command
}

func newDelegationAcceptCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "accept TASK_ID", Short: "accept a submitted task and create its git worktree",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return withDelegationService(command, settings, args[0], func(
				ctx context.Context,
				service *delegationworker.Service,
				task delegation.Task,
			) error {
				base := strings.TrimSpace(settings.GetString("delegation.accept.base_commit"))
				if base == "" {
					base = taskBaseCommit(task.Metadata)
				}
				if base == "" {
					return errors.New("base commit is absent from task metadata; pass --base-commit")
				}
				result, err := service.Accept(ctx, delegationworker.AcceptInput{
					TaskID: task.ID, ExpectedRevision: task.Revision, BaseCommit: base,
					Data: []byte(strings.TrimSpace(settings.GetString("delegation.accept.note"))),
				})
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), struct {
					Task     delegation.Task `json:"task"`
					Worktree worktree.Task   `json:"worktree"`
					Replay   bool            `json:"replay"`
				}{result.Task, result.Worktree, result.Replay})
			})
		},
	}
	command.Flags().String("base-commit", "", "git commit shared by the caller; inferred from task metadata when present")
	command.Flags().String("note", "", "audit note attached to the accept event")
	mustBind(settings, "delegation.accept.base_commit", command.Flags().Lookup("base-commit"))
	mustBind(settings, "delegation.accept.note", command.Flags().Lookup("note"))
	return command
}

func newDelegationDeliverCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "deliver TASK_ID", Short: "commit the current worktree changes and deliver a cumulative patch",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return withDelegationService(command, settings, args[0], func(
				ctx context.Context,
				service *delegationworker.Service,
				task delegation.Task,
			) error {
				result, err := service.Deliver(ctx, delegationworker.DeliverInput{
					TaskID: task.ID, ExpectedRevision: task.Revision,
					Data: []byte(strings.TrimSpace(settings.GetString("delegation.deliver.note"))),
				})
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), struct {
					Task     delegation.Task       `json:"task"`
					Artifact deliveredArtifactView `json:"artifact"`
					Local    localArtifactView     `json:"local"`
					Replay   bool                  `json:"replay"`
				}{
					Task: result.Task,
					Artifact: deliveredArtifactView{
						ID: result.StoredArtifact.ID, Turn: result.StoredArtifact.TurnNumber,
						MediaType: result.StoredArtifact.MediaType, SHA256: result.StoredArtifact.SHA256,
					},
					Local: localArtifactView{
						Commit:         result.WorktreeArtifact.Commit,
						PreviousCommit: result.WorktreeArtifact.PreviousCommit,
						Files:          append([]worktree.File(nil), result.WorktreeArtifact.Files...),
					},
					Replay: result.Replay,
				})
			})
		},
	}
	command.Flags().String("note", "", "audit note attached to the delivery event")
	mustBind(settings, "delegation.deliver.note", command.Flags().Lookup("note"))
	return command
}

func newDelegationExecCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "exec TASK_ID", Short: "request caller approval for a command on an owned task",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			requestID := strings.TrimSpace(settings.GetString("delegation.exec.request_id"))
			commandText := settings.GetString("delegation.exec.command")
			reason := strings.TrimSpace(settings.GetString("delegation.exec.reason"))
			cwd := strings.TrimSpace(settings.GetString("delegation.exec.cwd"))
			commandTimeout := settings.GetDuration("delegation.exec.timeout")
			if requestID == "" || strings.TrimSpace(commandText) == "" || reason == "" {
				return errors.New("exec request id, command, and reason are required")
			}
			if commandTimeout < 0 || commandTimeout > time.Hour || commandTimeout%time.Millisecond != 0 {
				return errors.New("exec command timeout must be a whole number of milliseconds from 0 through 1h")
			}
			return withDelegationClient(command, settings, args[0], func(
				ctx context.Context, client *delegationclient.Client, task delegation.Task,
			) error {
				result, err := client.RequestExec(ctx, delegation.RequestExecInput{
					CommandInput: delegation.CommandInput{TaskID: task.ID, ExpectedRevision: task.Revision},
					WorkerID:     client.WorkerID(),
					RequestID:    requestID,
					Command:      commandText,
					CWD:          cwd,
					TimeoutMS:    commandTimeout.Milliseconds(),
					Reason:       reason,
				})
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), result)
			})
		},
	}
	command.Flags().String("request-id", "", "stable idempotency key for this command request")
	command.Flags().String("command", "", "command proposed to the caller-side executor")
	command.Flags().String("cwd", "", "caller workspace-relative working directory")
	command.Flags().Duration("command-timeout", 30*time.Second, "bounded caller-side execution timeout (0 disables the request timeout)")
	command.Flags().String("reason", "", "why this command is needed for the task")
	mustBind(settings, "delegation.exec.request_id", command.Flags().Lookup("request-id"))
	mustBind(settings, "delegation.exec.command", command.Flags().Lookup("command"))
	mustBind(settings, "delegation.exec.cwd", command.Flags().Lookup("cwd"))
	mustBind(settings, "delegation.exec.timeout", command.Flags().Lookup("command-timeout"))
	mustBind(settings, "delegation.exec.reason", command.Flags().Lookup("reason"))
	return command
}

func newDelegationRejectCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "reject TASK_ID", Short: "reject a submitted task without creating a worktree",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return withDelegationClient(command, settings, args[0], func(
				ctx context.Context, client *delegationclient.Client, task delegation.Task,
			) error {
				result, err := client.RejectTask(ctx, delegation.CommandInput{
					TaskID: task.ID, ExpectedRevision: task.Revision,
					Data: []byte(strings.TrimSpace(settings.GetString("delegation.reject.reason"))),
				})
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), result)
			})
		},
	}
	command.Flags().String("reason", "", "rejection reason recorded in the audit event")
	mustBind(settings, "delegation.reject.reason", command.Flags().Lookup("reason"))
	return command
}

func newDelegationTransitionCommand(settings *viper.Viper, transition string) *cobra.Command {
	command := &cobra.Command{
		Use: transition + " TASK_ID", Short: transition + " an owned delegation task",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if transition == "complete" {
				return withDelegationService(command, settings, args[0], func(
					ctx context.Context, service *delegationworker.Service, task delegation.Task,
				) error {
					result, err := service.Complete(ctx, delegationworker.CompleteInput{
						TaskID: task.ID, ExpectedRevision: task.Revision,
						Data: []byte(strings.TrimSpace(settings.GetString("delegation.complete.reason"))),
					})
					if err != nil {
						return err
					}
					return writeJSON(command.OutOrStdout(), result)
				})
			}
			return withDelegationClient(command, settings, args[0], func(
				ctx context.Context, client *delegationclient.Client, task delegation.Task,
			) error {
				input := delegation.CommandInput{
					TaskID: task.ID, ExpectedRevision: task.Revision,
					Data: []byte(strings.TrimSpace(settings.GetString("delegation." + transition + ".reason"))),
				}
				result, err := client.FailTask(ctx, input)
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), result)
			})
		},
	}
	command.Flags().String("reason", "", transition+" reason recorded in the audit event")
	mustBind(settings, "delegation."+transition+".reason", command.Flags().Lookup("reason"))
	return command
}

func newDelegationRewindCommand(settings *viper.Viper) *cobra.Command {
	rewind := &cobra.Command{Use: "rewind", Short: "confirm or reject a caller rewind request"}
	confirm := &cobra.Command{
		Use: "confirm TASK_ID", Short: "snapshot local WIP, rewind git, then confirm the authority transition",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return withDelegationService(command, settings, args[0], func(
				ctx context.Context,
				service *delegationworker.Service,
				task delegation.Task,
			) error {
				target := settings.GetInt64("delegation.rewind.target_turn")
				if target < 0 {
					if task.PendingRewindTo == nil {
						return errors.New("task has no pending rewind target; pass --to-turn only to assert one")
					}
					target = *task.PendingRewindTo
				}
				if task.PendingRewindTo == nil || *task.PendingRewindTo != target {
					return fmt.Errorf("pending rewind target does not match %d", target)
				}
				result, err := service.ConfirmRewind(ctx, delegationworker.ConfirmRewindInput{
					TaskID: task.ID, ExpectedRevision: task.Revision, TargetTurn: target,
					Data: []byte(strings.TrimSpace(settings.GetString("delegation.rewind.note"))),
				})
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), struct {
					Task     delegation.Task `json:"task"`
					Worktree worktree.Task   `json:"worktree"`
					Replay   bool            `json:"replay"`
				}{result.Task, result.Worktree, result.Replay})
			})
		},
	}
	confirm.Flags().Int64("to-turn", -1, "assert the pending rewind target; defaults to the authoritative request")
	confirm.Flags().String("note", "", "audit note attached to the rewind confirmation")
	mustBind(settings, "delegation.rewind.target_turn", confirm.Flags().Lookup("to-turn"))
	mustBind(settings, "delegation.rewind.note", confirm.Flags().Lookup("note"))

	reject := &cobra.Command{
		Use: "reject TASK_ID", Short: "reject a pending rewind without changing local git",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return withDelegationClient(command, settings, args[0], func(
				ctx context.Context, client *delegationclient.Client, task delegation.Task,
			) error {
				result, err := client.RejectRewind(ctx, delegation.CommandInput{
					TaskID: task.ID, ExpectedRevision: task.Revision,
					Data: []byte(strings.TrimSpace(settings.GetString("delegation.rewind.reject_reason"))),
				})
				if err != nil {
					return err
				}
				return writeJSON(command.OutOrStdout(), result)
			})
		},
	}
	reject.Flags().String("reason", "", "reason for refusing the requested rewind")
	mustBind(settings, "delegation.rewind.reject_reason", reject.Flags().Lookup("reason"))
	rewind.AddCommand(confirm, reject)
	return rewind
}

func connectDelegationClient(ctx context.Context, settings *viper.Viper) (*delegationclient.Client, error) {
	gateway := strings.TrimSpace(settings.GetString("delegation.gateway"))
	token := strings.TrimSpace(settings.GetString("delegation.token"))
	if gateway == "" || token == "" {
		return nil, errors.New("delegation gateway and worker token are required")
	}
	outbox, err := resolvePrivatePath(settings.GetString("delegation.outbox"), "delegation worker outbox")
	if err != nil {
		return nil, err
	}
	timeout := settings.GetDuration("delegation.timeout")
	if timeout <= 0 {
		return nil, errors.New("delegation timeout must be positive")
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := delegationclient.Dial(connectCtx, delegationclient.Config{
		URL: gateway, Token: token, OutboxPath: outbox,
	})
	if err != nil {
		return nil, fmt.Errorf("connect delegation worker WebSocket: %w", err)
	}
	select {
	case <-connectCtx.Done():
		client.Close()
		return nil, fmt.Errorf("wait for delegation worker registration: %w", connectCtx.Err())
	case <-client.Ready():
	}
	if strings.TrimSpace(client.WorkerID()) == "" {
		client.Close()
		return nil, errors.New("delegation gateway returned an empty worker identity")
	}
	return client, nil
}

func withDelegationClient(
	command *cobra.Command,
	settings *viper.Viper,
	taskID string,
	action func(context.Context, *delegationclient.Client, delegation.Task) error,
) error {
	ctx, stop := operationContext(command.Context(), settings)
	defer stop()
	client, err := connectDelegationClient(ctx, settings)
	if err != nil {
		return err
	}
	defer client.Close()
	task, err := waitDelegationTask(ctx, client, strings.TrimSpace(taskID))
	if err != nil {
		return err
	}
	return action(ctx, client, task)
}

func withDelegationService(
	command *cobra.Command,
	settings *viper.Viper,
	taskID string,
	action func(context.Context, *delegationworker.Service, delegation.Task) error,
) error {
	return withDelegationClient(command, settings, taskID, func(
		ctx context.Context, client *delegationclient.Client, task delegation.Task,
	) error {
		engine, err := openDelegationWorktrees(settings)
		if err != nil {
			return err
		}
		service, err := delegationworker.New(delegationworker.Config{
			Authority: client, Worktrees: engine, WorkerID: client.WorkerID(),
		})
		if err != nil {
			return err
		}
		return action(ctx, service, task)
	})
}

func openDelegationWorktrees(settings *viper.Viper) (*worktree.Engine, error) {
	repository, err := resolvePrivatePath(settings.GetString("delegation.repository"), "delegation repository")
	if err != nil {
		return nil, err
	}
	worktreeRoot := strings.TrimSpace(settings.GetString("delegation.worktree_root"))
	if worktreeRoot != "" {
		worktreeRoot, err = resolvePrivatePath(worktreeRoot, "delegation worktree root")
		if err != nil {
			return nil, err
		}
	}
	return worktree.Open(worktree.Config{
		Repository: repository, WorktreeRoot: worktreeRoot,
		SharedRemote: strings.TrimSpace(settings.GetString("delegation.shared_remote")),
		SetupHook:    append([]string(nil), settings.GetStringSlice("delegation.setup_args")...),
		MaxFileBytes: settings.GetInt64("delegation.max_file_bytes"),
		AuthorName:   strings.TrimSpace(settings.GetString("delegation.author_name")),
		AuthorEmail:  strings.TrimSpace(settings.GetString("delegation.author_email")),
	})
}

func operationContext(parent context.Context, settings *viper.Viper) (context.Context, context.CancelFunc) {
	signalContext, stopSignals := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	timeout := settings.GetDuration("delegation.timeout")
	if timeout <= 0 {
		return signalContext, stopSignals
	}
	ctx, cancel := context.WithTimeout(signalContext, timeout)
	return ctx, func() {
		cancel()
		stopSignals()
	}
}

func waitDelegationTask(
	ctx context.Context,
	client *delegationclient.Client,
	taskID string,
) (delegation.Task, error) {
	if taskID == "" {
		return delegation.Task{}, errors.New("task id is required")
	}
	for {
		task, err := client.GetTask(ctx, taskID)
		if err == nil {
			return task, nil
		}
		if !errors.Is(err, delegation.ErrNotFound) {
			return delegation.Task{}, err
		}
		select {
		case <-ctx.Done():
			return delegation.Task{}, fmt.Errorf("wait for task %q recovery: %w", taskID, ctx.Err())
		case update, open := <-client.Updates():
			if !open {
				return delegation.Task{}, errors.New("delegation worker connection closed")
			}
			if update.RemovedTaskID == taskID {
				return delegation.Task{}, fmt.Errorf("%w: task %q is no longer recoverable", delegation.ErrNotFound, taskID)
			}
		}
	}
}

type delegationTaskView struct {
	ID              string    `json:"id"`
	State           string    `json:"state"`
	Revision        int64     `json:"revision"`
	WorkerID        string    `json:"worker_id,omitempty"`
	LatestTurn      int64     `json:"latest_turn"`
	NextTurn        int64     `json:"next_turn"`
	PendingRewindTo *int64    `json:"pending_rewind_to,omitempty"`
	BaseCommit      string    `json:"base_commit,omitempty"`
	MessageCount    int       `json:"message_count"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type deliveredArtifactView struct {
	ID        string `json:"id"`
	Turn      int64  `json:"turn"`
	MediaType string `json:"media_type"`
	SHA256    string `json:"sha256"`
}

type localArtifactView struct {
	Commit         string          `json:"commit"`
	PreviousCommit string          `json:"previous_commit,omitempty"`
	Files          []worktree.File `json:"files"`
}

func collectRecoveredTasks(
	ctx context.Context,
	client *delegationclient.Client,
	settle time.Duration,
) ([]delegationTaskView, error) {
	views := make(map[string]delegationTaskView)
	timer := time.NewTimer(settle)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			result := make([]delegationTaskView, 0, len(views))
			for _, view := range views {
				result = append(result, view)
			}
			sort.Slice(result, func(left, right int) bool {
				if result[left].UpdatedAt.Equal(result[right].UpdatedAt) {
					return result[left].ID < result[right].ID
				}
				return result[left].UpdatedAt.Before(result[right].UpdatedAt)
			})
			return result, nil
		case update, open := <-client.Updates():
			if !open {
				return nil, errors.New("delegation worker connection closed during recovery")
			}
			changed := false
			if update.Snapshot != nil {
				snapshot := update.Snapshot.Snapshot
				task := snapshot.Task
				views[task.ID] = delegationTaskView{
					ID: task.ID, State: string(task.State), Revision: task.Revision,
					WorkerID: task.WorkerID, LatestTurn: task.LatestTurn, NextTurn: task.NextTurn,
					PendingRewindTo: task.PendingRewindTo, BaseCommit: taskBaseCommit(task.Metadata),
					MessageCount: len(snapshot.Messages), UpdatedAt: task.UpdatedAt,
				}
				changed = true
			}
			if update.RemovedTaskID != "" {
				delete(views, update.RemovedTaskID)
				changed = true
			}
			if changed {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(settle)
			}
		}
	}
}

func taskBaseCommit(metadata []byte) string {
	if len(metadata) == 0 {
		return ""
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(metadata, &envelope) != nil {
		return ""
	}
	raw := envelope[delegation.RequestMetadataKey]
	if len(raw) == 0 {
		return ""
	}
	var request struct {
		BaseCommit       string `json:"baseCommit"`
		LegacyBaseCommit string `json:"base_commit"`
	}
	if json.Unmarshal(raw, &request) != nil {
		return ""
	}
	if value := strings.TrimSpace(request.BaseCommit); value != "" {
		return value
	}
	return strings.TrimSpace(request.LegacyBaseCommit)
}

func encodeDelegationUpdate(encoder *json.Encoder, update delegationclient.Update) error {
	switch {
	case update.Snapshot != nil:
		return encoder.Encode(struct {
			Type     string               `json:"type"`
			Snapshot workerproto.Snapshot `json:"snapshot"`
		}{"snapshot", *update.Snapshot})
	case update.RemovedTaskID != "":
		return encoder.Encode(map[string]any{"type": "removed", "task_id": update.RemovedTaskID})
	case update.Result != nil:
		return encoder.Encode(struct {
			Type   string                    `json:"type"`
			Result workerproto.CommandResult `json:"result"`
		}{"command_result", *update.Result})
	case update.Err != nil:
		return encoder.Encode(map[string]any{"type": "connection_error", "error": update.Err.Error()})
	default:
		return nil
	}
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
