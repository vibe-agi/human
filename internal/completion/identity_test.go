package completion

import "testing"

func TestRoutingIdentityValidate(t *testing.T) {
	t.Parallel()

	base := RoutingIdentity{
		CallerID:       "caller-1",
		WorkspaceKey:   "workspace:demo",
		TaskID:         "task-1",
		IdempotencyKey: "request-1",
		HarnessID:      "codex",
		HarnessVersion: "1.2.3",
		Root:           "/work/demo",
	}

	tests := []struct {
		name    string
		tier    CapabilityTier
		mutate  func(*RoutingIdentity)
		wantErr bool
	}{
		{name: "chat only needs caller", tier: TierChat},
		{name: "remote complete", tier: TierRemoteTools},
		{name: "workspace complete", tier: TierWorkspace},
		{name: "remote missing task", tier: TierRemoteTools, mutate: func(id *RoutingIdentity) { id.TaskID = "" }, wantErr: true},
		{name: "remote missing idempotency", tier: TierRemoteTools, mutate: func(id *RoutingIdentity) { id.IdempotencyKey = "" }, wantErr: true},
		{name: "invalid caller", tier: TierChat, mutate: func(id *RoutingIdentity) { id.CallerID = "../caller" }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := base
			if tt.mutate != nil {
				tt.mutate(&id)
			}
			err := id.Validate(tt.tier)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRoutingIdentityNamespace(t *testing.T) {
	t.Parallel()
	id := RoutingIdentity{CallerID: "c", WorkspaceKey: "w", TaskID: "t"}
	if got, want := id.Namespace(), "c/w/t"; got != want {
		t.Fatalf("Namespace() = %q, want %q", got, want)
	}
}

func TestParseCapabilityTier(t *testing.T) {
	t.Parallel()
	if got, err := ParseCapabilityTier(""); err != nil || got != TierChat {
		t.Fatalf("empty tier = %q, %v", got, err)
	}
	if _, err := ParseCapabilityTier("admin"); err == nil {
		t.Fatal("unknown tier accepted")
	}
}
