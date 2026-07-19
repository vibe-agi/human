package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/workspace"
	_ "modernc.org/sqlite"
)

const (
	agentCrashChildEnv = "HUMAN_AGENT_SQLITE_JOURNAL_CRASH_CHILD"
	agentCrashPathEnv  = "HUMAN_AGENT_SQLITE_JOURNAL_CRASH_PATH"
	agentCrashExitCode = 87
)

func TestJournalConformance(t *testing.T) {
	humantest.TestAgentWorkerJournal(t, func(
		ctx context.Context,
		test testing.TB,
	) (workerws.Journal, framework.ReleaseFunc, error) {
		resource, err := Open(ctx, Config{Path: filepath.Join(test.TempDir(), "journal.db")})
		if err != nil {
			return nil, nil, err
		}
		journal, err := resource.Value()
		if err != nil {
			_ = resource.Release(context.Background())
			return nil, nil, err
		}
		return journal, resource.Release, nil
	})
}

func TestJournalRecoveryFaultMatrix(t *testing.T) {
	humantest.TestAgentWorkerJournalRecovery(t, func(
		_ context.Context,
		test testing.TB,
	) (humantest.AgentWorkerJournalRecoveryOpener, error) {
		path := filepath.Join(test.TempDir(), "journal-recovery.db")
		return func(ctx context.Context) (
			workerws.Journal,
			framework.ReleaseFunc,
			error,
		) {
			resource, err := Open(ctx, Config{Path: path})
			if err != nil {
				return nil, nil, err
			}
			journal, err := resource.Value()
			if err != nil {
				_ = resource.Release(context.Background())
				return nil, nil, err
			}
			return journal, resource.Release, nil
		}, nil
	})
}

func TestJournalRecoversCommittedStateAfterAbruptProcessExit(t *testing.T) {
	if os.Getenv(agentCrashChildEnv) == "1" {
		if err := seedAgentJournalBeforeCrash(os.Getenv(agentCrashPathEnv)); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(agentCrashExitCode + 1)
		}
		// Intentionally bypass every Release, defer, and testing cleanup. The
		// parent process must recover only writes that were durable on return.
		os.Exit(agentCrashExitCode)
	}

	path := filepath.Join(t.TempDir(), "journal-crash.db")
	command := exec.Command(os.Args[0], "-test.run=^TestJournalRecoversCommittedStateAfterAbruptProcessExit$", "-test.count=1")
	command.Env = append(os.Environ(), agentCrashChildEnv+"=1", agentCrashPathEnv+"="+path)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != agentCrashExitCode {
		t.Fatalf("crash child = %v (exit %v), want %d; output:\n%s", err, exitCode(exit), agentCrashExitCode, output)
	}

	resource, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatalf("open Journal after process crash: %v", err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	journal, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Bind(t.Context(), sqliteTestBinding()); err != nil {
		t.Fatalf("durable binding after process crash: %v", err)
	}

	assignment := sqliteTestAssignment("crash-assignment")
	assignments, err := journal.ListAssignments(t.Context(), 0, 10)
	if err != nil || len(assignments) != 1 || assignments[0].Digest != sqliteTestDigest(assignment) ||
		!reflect.DeepEqual(assignments[0].Delivery, assignment) {
		t.Fatalf("assignments after process crash = %#v, %v", assignments, err)
	}
	divergentAssignment := assignment
	divergentAssignment.Assignment.Task.Revision++
	divergentAssignment.Assignment.Task.EventCount++
	if _, err := journal.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(divergentAssignment), Delivery: divergentAssignment,
	}); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent assignment after crash = %v, want ErrJournalConflict", err)
	}

	ackEvent := sqliteTestEvent("crash-event-ack")
	nackEvent := sqliteTestEvent("crash-event-nack")
	follower := sqliteTestEvent("crash-event-follower")
	events, err := journal.ListEvents(t.Context(), 0, 10)
	if err != nil || len(events) != 1 || events[0].Digest != sqliteTestDigest(follower) ||
		!reflect.DeepEqual(events[0].Delivery, follower) {
		t.Fatalf("pending follower after process crash = %#v, %v", events, err)
	}
	rejections, err := journal.ListRejections(t.Context(), 0, 10)
	wantNACK := sqliteTestNACK(nackEvent)
	if err != nil || len(rejections) != 1 || rejections[0].EventDigest != sqliteTestDigest(nackEvent) ||
		rejections[0].ReceiptDigest != sqliteTestDigest(wantNACK) ||
		!reflect.DeepEqual(rejections[0].Delivery, nackEvent) || rejections[0].Receipt != wantNACK {
		t.Fatalf("rejection after process crash = %#v, %v", rejections, err)
	}
	for _, settled := range []agent.WorkerEventDelivery{ackEvent, nackEvent} {
		if state, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
			Digest: sqliteTestDigest(settled), Delivery: settled,
		}); err != nil || state != workerws.JournalEntrySettled {
			t.Fatalf("settled event %q after crash = (%q, %v)", settled.ID, state, err)
		}
	}
	ackReceipt := agent.WorkerEventReceipt{
		Delivery: ackEvent.ID, Event: ackEvent.Event.ID, Decision: agent.WorkerEventACK,
	}
	if err := journal.SettleEvent(
		t.Context(), sqliteTestNACK(ackEvent), sqliteTestDigest(ackEvent), sqliteTestDigest(sqliteTestNACK(ackEvent)),
	); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent receipt after crash = %v, want ErrJournalConflict", err)
	}
	if err := journal.SettleEvent(
		t.Context(), ackReceipt, sqliteTestDigest(ackEvent), sqliteTestDigest(ackReceipt),
	); err != nil {
		t.Fatalf("exact ACK receipt after crash: %v", err)
	}
	maximum := assignments[0].Sequence
	if events[0].Sequence > maximum {
		maximum = events[0].Sequence
	}
	if rejections[0].Sequence > maximum {
		maximum = rejections[0].Sequence
	}
	fresh := sqliteTestEvent("crash-event-new")
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(fresh), Delivery: fresh,
	}); err != nil {
		t.Fatal(err)
	}
	events, err = journal.ListEvents(t.Context(), events[0].Sequence, 10)
	if err != nil || len(events) != 1 || events[0].Sequence <= maximum || !reflect.DeepEqual(events[0].Delivery, fresh) {
		t.Fatalf("new sequence after process crash = %#v, %v; want > %d", events, err, maximum)
	}
}

func seedAgentJournalBeforeCrash(path string) error {
	resource, err := Open(context.Background(), Config{Path: path})
	if err != nil {
		return err
	}
	journal, err := resource.Value()
	if err != nil {
		return err
	}
	if err := journal.Bind(context.Background(), sqliteTestBinding()); err != nil {
		return err
	}
	assignment := sqliteTestAssignment("crash-assignment")
	if _, err := journal.PutAssignment(context.Background(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(assignment), Delivery: assignment,
	}); err != nil {
		return err
	}
	ackEvent := sqliteTestEvent("crash-event-ack")
	if _, err := journal.PutEvent(context.Background(), workerws.JournalEvent{
		Digest: sqliteTestDigest(ackEvent), Delivery: ackEvent,
	}); err != nil {
		return err
	}
	ack := agent.WorkerEventReceipt{
		Delivery: ackEvent.ID, Event: ackEvent.Event.ID, Decision: agent.WorkerEventACK,
	}
	if err := journal.SettleEvent(context.Background(), ack, sqliteTestDigest(ackEvent), sqliteTestDigest(ack)); err != nil {
		return err
	}
	nackEvent := sqliteTestEvent("crash-event-nack")
	if _, err := journal.PutEvent(context.Background(), workerws.JournalEvent{
		Digest: sqliteTestDigest(nackEvent), Delivery: nackEvent,
	}); err != nil {
		return err
	}
	nack := sqliteTestNACK(nackEvent)
	if err := journal.SettleEvent(context.Background(), nack, sqliteTestDigest(nackEvent), sqliteTestDigest(nack)); err != nil {
		return err
	}
	follower := sqliteTestEvent("crash-event-follower")
	_, err = journal.PutEvent(context.Background(), workerws.JournalEvent{
		Digest: sqliteTestDigest(follower), Delivery: follower,
	})
	return err
}

func exitCode(err *exec.ExitError) int {
	if err == nil {
		return 0
	}
	return err.ExitCode()
}

func TestOwnedResourcePersistsBindingAndRecordsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	resource, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if !resource.Owned() {
		t.Fatal("SQLite Journal returned a borrowed Resource")
	}
	journal, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if got := journal.Description().Provider; got != "sqlite" {
		t.Fatalf("Journal provider = %q, want sqlite", got)
	}
	binding := sqliteTestBinding()
	if err := journal.Bind(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	assignment := sqliteTestAssignment("assignment-persisted")
	if state, err := journal.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(assignment), Delivery: assignment,
	}); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutAssignment = (%q, %v), want pending", state, err)
	}

	if second, err := Open(t.Context(), Config{Path: path}); !errors.Is(err, ErrDatabaseInUse) {
		if err == nil {
			_ = second.Release(t.Context())
		}
		t.Fatalf("second Open error = %v, want ErrDatabaseInUse", err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatalf("idempotent Resource release: %v", err)
	}
	if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
		t.Fatalf("Value after release = %v, want ErrResourceReleased", err)
	}
	if _, err := journal.ListAssignments(t.Context(), 0, 1); !errors.Is(err, workerws.ErrJournalClosed) {
		t.Fatalf("retained Journal after release = %v, want ErrJournalClosed", err)
	}

	reopened, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatalf("reopen Journal: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Release(context.Background()) })
	reopenedJournal, err := reopened.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedJournal.Bind(t.Context(), binding); err != nil {
		t.Fatalf("rebind exact identity: %v", err)
	}
	divergent := binding
	divergent.Gateway = "gateway-b"
	if err := reopenedJournal.Bind(t.Context(), divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("rebind different identity = %v, want ErrJournalConflict", err)
	}
	records, err := reopenedJournal.ListAssignments(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Delivery.ID != assignment.ID {
		t.Fatalf("persisted assignments = %#v, want %q", records, assignment.ID)
	}
}

func TestOpenSecuresFileAndRejectsSharedMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	resource, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("database mode = %04o, want 0600", got)
		}
	}
	port, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	concrete := port.(*journal)
	var journalMode string
	if err := concrete.database.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(t.Context(), Config{Path: "file:shared-journal?mode=memory&cache=shared"}); err == nil {
		t.Fatal("Open shared-memory SQLite unexpectedly succeeded")
	}
	independent, err := Open(t.Context(), Config{Path: ":memory:"})
	if err != nil {
		t.Fatalf("Open independent in-memory Journal: %v", err)
	}
	if err := independent.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestStrictPayloadDecodeAndStoredIdentityChecks(t *testing.T) {
	journal, _ := openBoundSQLiteTestJournal(t)
	event := sqliteTestEvent("event-strict-decode")
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(event), Delivery: event,
	}); err != nil {
		t.Fatal(err)
	}
	var original []byte
	if err := journal.database.QueryRowContext(t.Context(), `
		SELECT payload FROM agent_worker_journal_events WHERE delivery_id = ?`, string(event.ID),
	).Scan(&original); err != nil {
		t.Fatal(err)
	}
	unknown := append(append([]byte(nil), original[:len(original)-1]...), []byte(`,"unexpected":true}`)...)
	if _, err := journal.database.ExecContext(t.Context(), `
		UPDATE agent_worker_journal_events SET payload = ? WHERE delivery_id = ?`, unknown, string(event.ID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.ListEvents(t.Context(), 0, 10); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("ListEvents unknown field = %v, want ErrJournalCorrupt", err)
	}

	trailing := append(append([]byte(nil), original...), []byte(` {}`)...)
	if _, err := journal.database.ExecContext(t.Context(), `
		UPDATE agent_worker_journal_events SET payload = ? WHERE delivery_id = ?`, trailing, string(event.ID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.ListEvents(t.Context(), 0, 10); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("ListEvents trailing value = %v, want ErrJournalCorrupt", err)
	}

	if _, err := journal.database.ExecContext(t.Context(), `
		UPDATE agent_worker_journal_events SET payload = ?, delivery_id = 'different-delivery'
		WHERE delivery_id = ?`, original, string(event.ID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.ListEvents(t.Context(), 0, 10); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("ListEvents row/payload identity mismatch = %v, want ErrJournalCorrupt", err)
	}
}

func TestSettlementCompactsPayloadsToDigestTombstones(t *testing.T) {
	journal, _ := openBoundSQLiteTestJournal(t)
	assignment := sqliteTestAssignment("assignment-compact")
	if _, err := journal.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(assignment), Delivery: assignment,
	}); err != nil {
		t.Fatal(err)
	}
	if err := journal.ConfirmAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	var assignmentPayloadPresent int
	if err := journal.database.QueryRowContext(t.Context(), `
		SELECT payload IS NOT NULL
		FROM agent_worker_journal_assignments
		WHERE delivery_id = ?`, string(assignment.ID),
	).Scan(&assignmentPayloadPresent); err != nil {
		t.Fatal(err)
	}
	if assignmentPayloadPresent != 0 {
		t.Fatal("settled assignment retained its payload")
	}

	event := sqliteTestEvent("event-compact")
	eventDigest := sqliteTestDigest(event)
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: eventDigest, Delivery: event,
	}); err != nil {
		t.Fatal(err)
	}
	receipt := sqliteTestNACK(event)
	if err := journal.SettleEvent(t.Context(), receipt, eventDigest, sqliteTestDigest(receipt)); err != nil {
		t.Fatal(err)
	}
	if err := journal.ConfirmRejection(t.Context(), event.ID); err != nil {
		t.Fatal(err)
	}
	var eventPayloadPresent int
	var rejectionPayloadPresent int
	if err := journal.database.QueryRowContext(t.Context(), `
		SELECT payload IS NOT NULL
		FROM agent_worker_journal_events
		WHERE delivery_id = ?`, string(event.ID),
	).Scan(&eventPayloadPresent); err != nil {
		t.Fatal(err)
	}
	if err := journal.database.QueryRowContext(t.Context(), `
		SELECT payload IS NOT NULL
		FROM agent_worker_journal_rejections
		WHERE delivery_id = ?`, string(event.ID),
	).Scan(&rejectionPayloadPresent); err != nil {
		t.Fatal(err)
	}
	if eventPayloadPresent != 0 || rejectionPayloadPresent != 0 {
		t.Fatalf("compacted event/rejection retained payloads: %d/%d", eventPayloadPresent, rejectionPayloadPresent)
	}
}

func TestOpenRejectsForeignAndDriftedSchemas(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "foreign",
			mutate: func(t *testing.T, path string) {
				database, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				if _, err := database.Exec(`CREATE TABLE unrelated (id INTEGER PRIMARY KEY)`); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra_table",
			mutate: func(t *testing.T, path string) {
				resource, err := Open(t.Context(), Config{Path: path})
				if err != nil {
					t.Fatal(err)
				}
				if err := resource.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
				database, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				if _, err := database.Exec(`CREATE TABLE schema_drift (id INTEGER PRIMARY KEY)`); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			test.mutate(t, path)
			if resource, err := Open(t.Context(), Config{Path: path}); !errors.Is(err, ErrUnsupportedSchema) || !errors.Is(err, workerws.ErrJournalCorrupt) {
				if err == nil {
					_ = resource.Release(t.Context())
				}
				t.Fatalf("Open drifted schema = %v, want ErrUnsupportedSchema + ErrJournalCorrupt", err)
			}
		})
	}
}

func TestOpenRequiresPathAndLiveContext(t *testing.T) {
	if _, err := Open(t.Context(), Config{}); err == nil {
		t.Fatal("Open empty path unexpectedly succeeded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "journal.db")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancelled context = %v, want context.Canceled", err)
	}
	if _, err := Open(t.Context(), Config{Path: filepath.Join(t.TempDir(), "journal.db"), MaxPendingRecords: -1}); err == nil {
		t.Fatal("Open negative MaxPendingRecords unexpectedly succeeded")
	}
	if _, err := Open(t.Context(), Config{Path: filepath.Join(t.TempDir(), "journal.db"), MaxPendingBytes: -1}); err == nil {
		t.Fatal("Open negative MaxPendingBytes unexpectedly succeeded")
	}
}

func TestPendingRecordQuotaSpansAssignmentsEventsAndRejections(t *testing.T) {
	journal, _ := openConfiguredBoundSQLiteTestJournal(t, Config{
		Path: filepath.Join(t.TempDir(), "journal.db"), MaxPendingRecords: 2,
	})
	assignment := sqliteTestAssignment("quota-assignment-a")
	assignmentRecord := workerws.JournalAssignment{Digest: sqliteTestDigest(assignment), Delivery: assignment}
	if _, err := journal.PutAssignment(t.Context(), assignmentRecord); err != nil {
		t.Fatal(err)
	}
	event := sqliteTestEvent("quota-event-a")
	eventRecord := workerws.JournalEvent{Digest: sqliteTestDigest(event), Delivery: event}
	if _, err := journal.PutEvent(t.Context(), eventRecord); err != nil {
		t.Fatal(err)
	}
	blocked := sqliteTestAssignment("quota-assignment-b")
	if _, err := journal.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(blocked), Delivery: blocked,
	}); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("new assignment at record quota = %v, want ErrJournalLimit", err)
	}
	if state, err := journal.PutAssignment(t.Context(), assignmentRecord); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact replay at record quota = (%q, %v), want pending", state, err)
	}
	divergent := assignmentRecord
	divergent.Digest = differentSQLiteTestDigest(divergent.Digest)
	if _, err := journal.PutAssignment(t.Context(), divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent replay at record quota = %v, want ErrJournalConflict", err)
	}

	receipt := sqliteTestNACK(event)
	if err := journal.SettleEvent(t.Context(), receipt, eventRecord.Digest, sqliteTestDigest(receipt)); err != nil {
		t.Fatalf("NACK must converge at quota: %v", err)
	}
	// The event moved to the rejection inbox, so the total pending payload
	// record count remains two and another new event is still rejected.
	secondEvent := sqliteTestEvent("quota-event-b")
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(secondEvent), Delivery: secondEvent,
	}); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("new event with pending rejection at quota = %v, want ErrJournalLimit", err)
	}
	if err := journal.ConfirmAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatalf("assignment compaction must converge at quota: %v", err)
	}
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(secondEvent), Delivery: secondEvent,
	}); err != nil {
		t.Fatalf("new event after compaction freed quota: %v", err)
	}
	if err := journal.ConfirmRejection(t.Context(), event.ID); err != nil {
		t.Fatalf("rejection compaction must converge at quota: %v", err)
	}
}

func TestPendingByteQuotaUsesEncodedPayloadBytes(t *testing.T) {
	assignment := sqliteTestAssignment("byte-quota-assignment")
	event := sqliteTestEvent("byte-quota-event")
	assignmentPayload, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	eventPayload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	limit := int64(len(assignmentPayload))
	if int64(len(eventPayload)) > limit {
		limit = int64(len(eventPayload))
	}
	journal, _ := openConfiguredBoundSQLiteTestJournal(t, Config{
		Path:              filepath.Join(t.TempDir(), "journal.db"),
		MaxPendingRecords: 10, MaxPendingBytes: limit,
	})
	assignmentRecord := workerws.JournalAssignment{Digest: sqliteTestDigest(assignment), Delivery: assignment}
	if _, err := journal.PutAssignment(t.Context(), assignmentRecord); err != nil {
		t.Fatalf("first payload within byte quota: %v", err)
	}
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(event), Delivery: event,
	}); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("combined encoded payload over byte quota = %v, want ErrJournalLimit", err)
	}
	if state, err := journal.PutAssignment(t.Context(), assignmentRecord); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact replay over byte quota = (%q, %v), want pending", state, err)
	}
	if err := journal.ConfirmAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(event), Delivery: event,
	}); err != nil {
		t.Fatalf("event after payload compaction freed byte quota: %v", err)
	}
}

func TestNACKCanTemporarilyExceedByteQuotaAndStillConverge(t *testing.T) {
	event := sqliteTestEvent("nack-byte-quota-a")
	eventPayload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	journal, _ := openConfiguredBoundSQLiteTestJournal(t, Config{
		Path:              filepath.Join(t.TempDir(), "journal.db"),
		MaxPendingRecords: 10, MaxPendingBytes: int64(len(eventPayload)),
	})
	eventDigest := sqliteTestDigest(event)
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: eventDigest, Delivery: event,
	}); err != nil {
		t.Fatal(err)
	}
	receipt := sqliteTestNACK(event)
	if err := journal.SettleEvent(t.Context(), receipt, eventDigest, sqliteTestDigest(receipt)); err != nil {
		t.Fatalf("NACK at byte quota must converge despite receipt overhead: %v", err)
	}
	var rejectionBytes int64
	if err := journal.database.QueryRowContext(t.Context(), `
		SELECT length(payload)
		FROM agent_worker_journal_rejections
		WHERE delivery_id = ?`, string(event.ID),
	).Scan(&rejectionBytes); err != nil {
		t.Fatal(err)
	}
	if rejectionBytes <= int64(len(eventPayload)) {
		t.Fatalf("rejection payload = %d bytes, want above configured %d-byte quota", rejectionBytes, len(eventPayload))
	}
	second := sqliteTestEvent("nack-byte-quota-b")
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(second), Delivery: second,
	}); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("new event while transiently over byte quota = %v, want ErrJournalLimit", err)
	}
	if err := journal.ConfirmRejection(t.Context(), event.ID); err != nil {
		t.Fatalf("ConfirmRejection while over byte quota: %v", err)
	}
	if _, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: sqliteTestDigest(second), Delivery: second,
	}); err != nil {
		t.Fatalf("new event after rejection compaction: %v", err)
	}
}

func TestReopenWithLowerQuotaRejectsOnlyNewRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	resource, err := Open(t.Context(), Config{Path: path, MaxPendingRecords: 3})
	if err != nil {
		t.Fatal(err)
	}
	port, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := port.Bind(t.Context(), sqliteTestBinding()); err != nil {
		t.Fatal(err)
	}
	first := sqliteTestAssignment("lower-quota-first")
	second := sqliteTestAssignment("lower-quota-second")
	for _, delivery := range []agent.WorkerAssignmentDelivery{first, second} {
		if _, err := port.PutAssignment(t.Context(), workerws.JournalAssignment{
			Digest: sqliteTestDigest(delivery), Delivery: delivery,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), Config{Path: path, MaxPendingRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Release(context.Background()) })
	port, err = reopened.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := port.Bind(t.Context(), sqliteTestBinding()); err != nil {
		t.Fatal(err)
	}
	if state, err := port.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(first), Delivery: first,
	}); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact replay over lowered quota = (%q, %v), want pending", state, err)
	}
	third := sqliteTestAssignment("lower-quota-third")
	if _, err := port.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(third), Delivery: third,
	}); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("new record over lowered quota = %v, want ErrJournalLimit", err)
	}
	if err := port.ConfirmAssignment(t.Context(), first.ID); err != nil {
		t.Fatalf("first compaction over lowered quota: %v", err)
	}
	if err := port.ConfirmAssignment(t.Context(), second.ID); err != nil {
		t.Fatalf("second compaction at lowered quota: %v", err)
	}
	if _, err := port.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: sqliteTestDigest(third), Delivery: third,
	}); err != nil {
		t.Fatalf("new record after convergence under lowered quota: %v", err)
	}
}

func TestEncodedArtifactAbove64MiBStillRoundTrips(t *testing.T) {
	const rawBytes = 48 << 20
	raw := make([]byte, rawBytes)
	raw[0], raw[len(raw)-1] = 0x12, 0x34
	event := sqliteTestFreezeEvent("large-artifact-event", raw)
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= 64<<20 {
		t.Fatalf("encoded event = %d bytes, want above the former 64 MiB ceiling", len(encoded))
	}
	if len(encoded) > maxJournalPayloadBytes {
		t.Fatalf("encoded event = %d bytes, exceeds the 96 MiB Journal ceiling", len(encoded))
	}
	encoded = nil
	runtime.GC()

	journal, _ := openConfiguredBoundSQLiteTestJournal(t, Config{
		Path:              filepath.Join(t.TempDir(), "journal.db"),
		MaxPendingRecords: 2, MaxPendingBytes: 128 << 20,
	})
	digest := workerws.JournalDigest("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if state, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
		Digest: digest, Delivery: event,
	}); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent with >64 MiB encoded payload = (%q, %v), want pending", state, err)
	}
	events, err := journal.ListEvents(t.Context(), 0, 1)
	if err != nil {
		t.Fatalf("ListEvents with >64 MiB encoded payload: %v", err)
	}
	if len(events) != 1 || events[0].Delivery.Event.Freeze == nil {
		t.Fatalf("large event round trip = %#v", events)
	}
	got := events[0].Delivery.Event.Freeze.Payload.Data
	if len(got) != rawBytes {
		t.Fatalf("large payload round trip length = %d, want %d", len(got), rawBytes)
	}
	if got[0] != 0x12 || got[len(got)-1] != 0x34 {
		t.Fatalf("large payload round trip endpoints = %x/%x", got[0], got[len(got)-1])
	}
}

func TestCommitUnknownAfterPutCanReconcileExactly(t *testing.T) {
	journal, resource := openBoundSQLiteTestJournal(t)
	assignment := sqliteTestAssignment("assignment-ambiguous")
	digest := sqliteTestDigest(assignment)
	injected := errors.New("injected commit reply loss")
	setSQLiteTestCommit(t, journal, func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return injected
	})
	if _, err := journal.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: digest, Delivery: assignment,
	}); !errors.Is(err, workerws.ErrJournalCommitUnknown) || !errors.Is(err, injected) {
		t.Fatalf("ambiguous PutAssignment = %v, want ErrJournalCommitUnknown + injected", err)
	}
	setSQLiteTestCommit(t, journal, nil)
	state, err := journal.PutAssignment(t.Context(), workerws.JournalAssignment{
		Digest: digest, Delivery: assignment,
	})
	if err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact reconciliation = (%q, %v), want pending", state, err)
	}
	records, err := journal.ListAssignments(t.Context(), 0, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("reconciled records = %#v, %v; want exactly one", records, err)
	}
	_ = resource
}

func TestSettleNACKRollbackAndAmbiguousCommitRemainAtomic(t *testing.T) {
	for _, test := range []struct {
		name        string
		commit      func(*sql.Tx) error
		wantPending bool
	}{
		{
			name: "rollback_before_commit",
			commit: func(*sql.Tx) error {
				return errors.New("injected pre-commit failure")
			},
			wantPending: true,
		},
		{
			name: "reply_lost_after_commit",
			commit: func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return errors.New("injected post-commit failure")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal, _ := openBoundSQLiteTestJournal(t)
			event := sqliteTestEvent(agent.WorkerDeliveryID("event-delivery-" + test.name))
			eventDigest := sqliteTestDigest(event)
			if state, err := journal.PutEvent(t.Context(), workerws.JournalEvent{
				Digest: eventDigest, Delivery: event,
			}); err != nil || state != workerws.JournalEntryPending {
				t.Fatalf("PutEvent = (%q, %v)", state, err)
			}
			receipt := sqliteTestNACK(event)
			receiptDigest := sqliteTestDigest(receipt)
			setSQLiteTestCommit(t, journal, test.commit)
			if err := journal.SettleEvent(t.Context(), receipt, eventDigest, receiptDigest); !errors.Is(err, workerws.ErrJournalCommitUnknown) {
				t.Fatalf("ambiguous SettleEvent = %v, want ErrJournalCommitUnknown", err)
			}
			setSQLiteTestCommit(t, journal, nil)

			events, err := journal.ListEvents(t.Context(), 0, 10)
			if err != nil {
				t.Fatal(err)
			}
			rejections, err := journal.ListRejections(t.Context(), 0, 10)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantPending {
				if len(events) != 1 || len(rejections) != 0 {
					t.Fatalf("rolled-back settlement events=%d rejections=%d, want 1/0", len(events), len(rejections))
				}
			} else if len(events) != 0 || len(rejections) != 1 {
				t.Fatalf("committed settlement events=%d rejections=%d, want 0/1", len(events), len(rejections))
			}

			if err := journal.SettleEvent(t.Context(), receipt, eventDigest, receiptDigest); err != nil {
				t.Fatalf("exact settlement reconciliation: %v", err)
			}
			rejections, err = journal.ListRejections(t.Context(), 0, 10)
			if err != nil || len(rejections) != 1 {
				t.Fatalf("reconciled rejections = %#v, %v; want exactly one", rejections, err)
			}
			if rejections[0].Delivery.ID != event.ID || rejections[0].Receipt != receipt {
				t.Fatalf("rejection lost exact delivery or receipt: %#v", rejections[0])
			}
		})
	}
}

func openBoundSQLiteTestJournal(t *testing.T) (*journal, framework.Resource[workerws.Journal]) {
	t.Helper()
	return openConfiguredBoundSQLiteTestJournal(t, Config{Path: filepath.Join(t.TempDir(), "journal.db")})
}

func openConfiguredBoundSQLiteTestJournal(
	t *testing.T,
	config Config,
) (*journal, framework.Resource[workerws.Journal]) {
	t.Helper()
	resource, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	port, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	journal, ok := port.(*journal)
	if !ok {
		t.Fatalf("official adapter concrete Journal = %T", port)
	}
	if err := journal.Bind(t.Context(), sqliteTestBinding()); err != nil {
		t.Fatal(err)
	}
	return journal, resource
}

func setSQLiteTestCommit(t *testing.T, journal *journal, commit func(*sql.Tx) error) {
	t.Helper()
	journal.lifecycle.Lock()
	defer journal.lifecycle.Unlock()
	if commit == nil {
		journal.commitTx = func(tx *sql.Tx) error { return tx.Commit() }
		return
	}
	journal.commitTx = commit
}

func sqliteTestBinding() workerws.JournalBinding {
	return workerws.JournalBinding{Gateway: "gateway-a", Authority: "tenant-a", Worker: "worker-a"}
}

func sqliteTestAssignment(id agent.WorkerDeliveryID) agent.WorkerAssignmentDelivery {
	now := time.Date(2026, 7, 19, 1, 2, 3, 4, time.UTC)
	ref := agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "tenant-a", ID: "workspace-a"},
		ID:        "task-a",
	}
	return agent.WorkerAssignmentDelivery{
		ID: id,
		Assignment: agent.LeaseAssignment{
			Grant: agent.LeaseGrant{Task: ref, Worker: "worker-a", Fence: 1},
			Task: agent.Task{
				Ref: ref, Context: agent.ContextRef{Authority: "tenant-a", ID: "context-a"},
				State: agent.TaskSubmitted, Revision: 1, MessageCount: 1, EventCount: 1,
				CreatedAt: now, UpdatedAt: now,
			},
			GrantedAt: now.Add(time.Nanosecond),
		},
	}
}

func sqliteTestEvent(id agent.WorkerDeliveryID) agent.WorkerEventDelivery {
	assignment := sqliteTestAssignment("assignment-for-event")
	return agent.WorkerEventDelivery{
		ID: id,
		Event: agent.WorkerEvent{
			ID: "command-" + agent.CommandID(id), Kind: agent.WorkerEventAcceptTask,
			Task: assignment.Assignment.Grant.Task, Fence: assignment.Assignment.Grant.Fence,
			ExpectedRevision: assignment.Assignment.Task.Revision,
		},
	}
}

func sqliteTestFreezeEvent(id agent.WorkerDeliveryID, payload []byte) agent.WorkerEventDelivery {
	assignment := sqliteTestAssignment("assignment-for-freeze")
	return agent.WorkerEventDelivery{
		ID: id,
		Event: agent.WorkerEvent{
			ID: "command-" + agent.CommandID(id), Kind: agent.WorkerEventFreezeArtifact,
			Task: assignment.Assignment.Grant.Task, Fence: assignment.Assignment.Grant.Fence,
			ExpectedRevision: assignment.Assignment.Task.Revision,
			Freeze: &agent.WorkerArtifactFreeze{
				Artifact: "artifact-large", ExpectedBaseRevision: "revision-a",
				Payload: workspace.Payload{MediaType: "application/octet-stream", Data: payload},
			},
		},
	}
}

func sqliteTestNACK(delivery agent.WorkerEventDelivery) agent.WorkerEventReceipt {
	return agent.WorkerEventReceipt{
		Delivery: delivery.ID, Event: delivery.Event.ID,
		Decision: agent.WorkerEventNACK, Code: agent.WorkerRejectState,
		Message: "deterministic test rejection",
	}
}

func sqliteTestDigest(value any) workerws.JournalDigest {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(payload)
	return workerws.JournalDigest(hex.EncodeToString(digest[:]))
}

func differentSQLiteTestDigest(digest workerws.JournalDigest) workerws.JournalDigest {
	if digest[0] == '0' {
		return "1" + digest[1:]
	}
	return "0" + digest[1:]
}
