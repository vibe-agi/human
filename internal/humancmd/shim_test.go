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
			args: []string{"--caller-token", "caller-token", "--tool-token", "tool-token", "--workspace-key", "workspace-1", "--task-id", "task-1"},
		},
		{
			name: "missing task id",
			args: []string{"--caller-token", "caller-token", "--tool-token", "tool-token", "--caller-id", "caller-1", "--workspace-key", "workspace-1"},
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
}
