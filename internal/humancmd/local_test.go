package humancmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/vibe-agi/human/gateway"
)

var errSimulatedCredentialCrash = errors.New("simulated credential rotation crash")

func TestLocalCredentialsRoundTripAndPermissions(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "private")
	path := filepath.Join(directory, "credentials.json")
	want := testCredentialState()
	if err := writeLocalCredentials(path, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := readLocalCredentials(path)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatalf("credentials = %+v, found=%v, err=%v", got, found, err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("credential mode = %v", info.Mode().Perm())
		}
	}
}

func TestLocalCredentialsRejectBroadPermissionsAndUnknownFields(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "credentials.json")
	valid := `{"version":1,"active":{"caller":{"type":"caller","subject_id":"c","key_id":"ck","secret":"cs"},"worker":{"type":"worker","subject_id":"w","key_id":"wk","secret":"ws"}}}`
	if err := os.WriteFile(path, []byte(valid), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readLocalCredentials(path); err == nil {
		t.Fatal("read accepted broad credential permissions")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	withUnknown := valid[:len(valid)-1] + `,"extra":true}`
	if err := os.WriteFile(path, []byte(withUnknown), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readLocalCredentials(path); err == nil {
		t.Fatal("read accepted unknown credential fields")
	}
}

func TestCredentialRotationRecoversEveryDurableSIGKILLCut(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		crashWrite    int
		crashActivate int
		crashRevoke   int
		rerunReset    bool
	}{
		{name: "prepared journal", crashWrite: 1, rerunReset: true},
		{name: "caller activated", crashActivate: 1, rerunReset: true},
		{name: "pair activated", crashActivate: 2, rerunReset: true},
		{name: "active switched", crashWrite: 2, rerunReset: true},
		{name: "worker revoking marked", crashWrite: 3, rerunReset: true},
		{name: "worker revoked", crashRevoke: 1, rerunReset: true},
		{name: "worker journal cleared", crashWrite: 4, rerunReset: true},
		{name: "caller revoking marked", crashWrite: 5, rerunReset: true},
		{name: "caller revoked", crashRevoke: 2, rerunReset: true},
		{name: "rotation complete", crashWrite: 6},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			databasePath := filepath.Join(t.TempDir(), "gateway.db")
			server := openCredentialTestGateway(t, databasePath)
			privateDirectory := filepath.Join(t.TempDir(), "private")
			if err := os.Mkdir(privateDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(privateDirectory, "credentials.json")
			old := activatedCredentialPair(t, server, "old-caller", "old-worker")
			initial := localCredentialFile{Version: localCredentialVersion, Active: &old}
			if err := writeLocalCredentials(path, initial); err != nil {
				t.Fatal(err)
			}

			crashing := &crashingCredentialAuthority{
				localCredentialAuthority: server,
				crashActivate:            test.crashActivate,
				crashRevoke:              test.crashRevoke,
			}
			writer := &crashingCredentialWriter{path: path, crashWrite: test.crashWrite}
			if _, err := reconcileLocalCredentialState(
				ctx, crashing, writer.write, initial, true, true, "new-caller", "new-worker",
			); !errors.Is(err, errSimulatedCredentialCrash) {
				t.Fatalf("rotation error = %v, want simulated crash", err)
			}

			crashed, found, err := readLocalCredentials(path)
			if err != nil || !found {
				t.Fatalf("read crash journal: found=%v err=%v", found, err)
			}
			if crashed.Active == nil {
				t.Fatal("crash left no durable active credential pair")
			}
			if err := validateLocalCredentialPair(ctx, server, *crashed.Active); err != nil {
				t.Fatalf("crash left no usable active credential pair: %v", err)
			}
			expectedActive := crashed.Active
			if crashed.Rotation != nil {
				expectedActive = crashed.Rotation
			}

			// Drop every in-memory object before recovery. The injected error is
			// positioned immediately after a durable filesystem/SQLite side
			// effect, which models SIGKILL at each ordering boundary.
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			server = openCredentialTestGateway(t, databasePath)
			recovered, err := reconcileLocalCredentialState(
				ctx, server,
				func(state localCredentialFile) error { return writeLocalCredentials(path, state) },
				crashed, true, test.rerunReset, "new-caller", "new-worker",
			)
			if err != nil {
				t.Fatalf("recover rotation: %v", err)
			}
			if recovered.Rotation != nil || len(recovered.PendingRevocations) != 0 {
				t.Fatalf("recovery left journal work: %+v", recovered)
			}
			if err := validateLocalCredentialPair(ctx, server, *recovered.Active); err != nil {
				t.Fatalf("recovered active pair: %v", err)
			}
			if recovered.Active.Caller.SubjectID != "new-caller" || recovered.Active.Worker.SubjectID != "new-worker" {
				t.Fatalf("recovered wrong pair: %+v", recovered.Active)
			}
			if recovered.Active.Caller.KeyID != expectedActive.Caller.KeyID ||
				recovered.Active.Worker.KeyID != expectedActive.Worker.KeyID {
				t.Fatalf("recovery repeated an in-progress reset: got %+v want %+v", recovered.Active, expectedActive)
			}
			for _, stale := range []localCredential{old.Caller, old.Worker} {
				if _, err := server.ValidateToken(ctx, stale.Secret); !errors.Is(err, gateway.ErrUnauthorized) {
					t.Fatalf("stale %s credential remains valid: %v", stale.Type, err)
				}
			}
		})
	}
}

func TestPendingRevocationNeverUsesTamperedKeyID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	server := openCredentialTestGateway(t, filepath.Join(t.TempDir(), "gateway.db"))
	active := activatedCredentialPair(t, server, "active-caller", "active-worker")
	unrelated := activatedCredentialPair(t, server, "other-caller", "other-worker")
	tampered := unrelated.Caller
	tampered.KeyID = "key_unrelated"
	state := localCredentialFile{
		Version: localCredentialVersion, Active: &active,
		PendingRevocations: []localCredential{tampered},
	}
	if _, err := drainLocalCredentialRevocations(ctx, server, func(localCredentialFile) error { return nil }, state); !errors.Is(err, gateway.ErrCredentialMismatch) {
		t.Fatalf("tampered pending revocation error = %v", err)
	}
	if _, err := server.ValidateToken(ctx, unrelated.Caller.Secret); err != nil {
		t.Fatalf("tampered journal revoked unrelated credential: %v", err)
	}
}

func testCredentialState() localCredentialFile {
	pair := localCredentialPair{
		Caller: localCredential{Type: gateway.PrincipalCaller, SubjectID: "caller", KeyID: "caller-key", Secret: "caller-secret"},
		Worker: localCredential{Type: gateway.PrincipalWorker, SubjectID: "worker", KeyID: "worker-key", Secret: "worker-secret"},
	}
	return localCredentialFile{Version: localCredentialVersion, Active: &pair}
}

func openCredentialTestGateway(t *testing.T, databasePath string) *gateway.Server {
	t.Helper()
	config := gateway.DefaultConfig()
	config.DatabasePath = databasePath
	server, err := gateway.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func activatedCredentialPair(
	t *testing.T,
	server *gateway.Server,
	callerSubject, workerSubject string,
) localCredentialPair {
	t.Helper()
	pair, err := prepareLocalCredentialPair(server, callerSubject, workerSubject)
	if err != nil {
		t.Fatal(err)
	}
	for _, credential := range []localCredential{pair.Caller, pair.Worker} {
		if err := server.ActivateToken(context.Background(), credential.prepared()); err != nil {
			t.Fatal(err)
		}
	}
	return pair
}

type crashingCredentialWriter struct {
	path       string
	crashWrite int
	writes     int
}

func (writer *crashingCredentialWriter) write(state localCredentialFile) error {
	if err := writeLocalCredentials(writer.path, state); err != nil {
		return err
	}
	writer.writes++
	if writer.writes == writer.crashWrite {
		return errSimulatedCredentialCrash
	}
	return nil
}

type crashingCredentialAuthority struct {
	localCredentialAuthority
	crashActivate int
	crashRevoke   int
	activations   int
	revocations   int
}

func (authority *crashingCredentialAuthority) ActivateToken(ctx context.Context, token gateway.PreparedToken) error {
	if err := authority.localCredentialAuthority.ActivateToken(ctx, token); err != nil {
		return err
	}
	authority.activations++
	if authority.activations == authority.crashActivate {
		return errSimulatedCredentialCrash
	}
	return nil
}

func (authority *crashingCredentialAuthority) RevokeToken(ctx context.Context, secret, keyID string) error {
	if err := authority.localCredentialAuthority.RevokeToken(ctx, secret, keyID); err != nil {
		return err
	}
	authority.revocations++
	if authority.revocations == authority.crashRevoke {
		return errSimulatedCredentialCrash
	}
	return nil
}
