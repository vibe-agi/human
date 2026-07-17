package humancmd

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestShimRequiresExplicitStableCallerAndTaskIdentity(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		args []string
	}{
		{
			name: "missing caller id",
			args: []string{"--workspace-key", "workspace-1", "--task-id", "task-1"},
		},
		{
			name: "missing task id",
			args: []string{"--caller-id", "caller-1", "--workspace-key", "workspace-1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := newShimCommand(viper.New())
			command.SetArgs(test.args)
			err := command.ExecuteContext(context.Background())
			if err == nil || !strings.Contains(err.Error(), "caller-id, workspace-key, and task-id are required") {
				t.Fatalf("shim identity error = %v", err)
			}
		})
	}

	command := newShimCommand(viper.New())
	if flag := command.Flags().Lookup("task-id"); flag == nil || strings.Contains(flag.Usage, "generated") {
		t.Fatalf("task-id flag still implies process-local generation: %#v", flag)
	}
	for _, name := range []string{"caller-token-file", "tool-token-file"} {
		if command.Flags().Lookup(name) == nil {
			t.Fatalf("shim flag %q is missing", name)
		}
	}
	for _, name := range []string{"caller-token", "tool-token"} {
		if command.Flags().Lookup(name) != nil {
			t.Fatalf("shim exposes plaintext secret flag %q", name)
		}
	}
}

func TestShimLedgerDefaultsToAutomaticWorkspaceScope(t *testing.T) {
	t.Parallel()
	command := newShimCommand(viper.New())
	flag := command.Flags().Lookup("ledger")
	if flag == nil || flag.DefValue != automaticPrivatePath {
		t.Fatalf("ledger default = %+v", flag)
	}
	if !strings.Contains(flag.Usage, "OS user data") || !strings.Contains(flag.Usage, "real workspace") {
		t.Fatalf("ledger help does not explain safe scope: %q", flag.Usage)
	}
}
