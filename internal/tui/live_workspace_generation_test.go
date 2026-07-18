package tui

import (
	"os"
	"path/filepath"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
)

func TestLiveWorkspaceSaveDrainsAfterResponseOutbox(t *testing.T) {
	for _, test := range []struct {
		name      string
		kind      pendingSendKind
		sendError error
		autoSend  bool
	}{
		{name: "accept commit", kind: pendingAccept},
		{name: "reply commit", kind: pendingReply, autoSend: true},
		{name: "command outbox failure", kind: pendingCommand, sendError: definitelyNotStored("outbox unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			model, assignment, namespace, events := liveWorkspaceGenerationFixture(t, test.autoSend)
			model.pending = pendingSend{
				kind: test.kind, eventID: "event-pending", assignment: assignment,
				context: cloneAssignment(model.lastContext), automatic: test.kind == pendingAccept,
			}
			if test.kind == pendingAccept || test.kind == pendingCommand {
				model.active = nil
			}

			updated, _ := model.Update(mirrorWatchEvent{
				namespace: namespace, events: events, event: workmirror.WatchEvent{}, open: true,
			})
			model = updated.(Model)
			if !model.mirrorDirty[namespace] || len(model.mirrorReviewing) != 0 {
				t.Fatalf("save was not queued behind pending %d: %+v", test.kind, model)
			}

			updated, review := model.Update(eventSent{eventID: "event-pending", err: test.sendError})
			model = updated.(Model)
			if review == nil || model.mirrorReviewing[namespace] != model.mirrorGeneration[namespace] {
				t.Fatalf("pending %d completion did not drain newest workspace generation: %+v", test.kind, model)
			}
			updated, followup := model.Update(commandResult(t, review))
			model = updated.(Model)
			if len(model.delivery.changes) != 1 {
				t.Fatalf("drained review = %+v", model.delivery)
			}
			if test.autoSend {
				if followup == nil || model.delivery.stage != deliveryConfirming {
					t.Fatalf("auto-send did not continue from drained review: %+v", model.delivery)
				}
			} else if followup != nil || model.delivery.stage != deliveryReviewed {
				t.Fatalf("review-only drain unexpectedly sent: stage=%d command=%v", model.delivery.stage, followup != nil)
			}
		})
	}
}

func TestLiveWorkspaceStaleReviewStartsLatestGeneration(t *testing.T) {
	model, assignment, namespace, events := liveWorkspaceGenerationFixture(t, false)
	updated, firstReview := model.startMirrorReview()
	model = updated.(Model)
	firstResult := commandResult(t, firstReview)
	firstGeneration := model.mirrorGeneration[namespace]

	if err := os.WriteFile(filepath.Join(model.mirrors[namespace].Dir(), "live.txt"), []byte("v2"), 0o600); err != nil {
		t.Fatal(err)
	}
	updated, _ = model.Update(mirrorWatchEvent{
		namespace: namespace, events: events, event: workmirror.WatchEvent{}, open: true,
	})
	model = updated.(Model)
	latestGeneration := model.mirrorGeneration[namespace]
	if latestGeneration <= firstGeneration {
		t.Fatalf("generation did not advance: first=%d latest=%d", firstGeneration, latestGeneration)
	}

	updated, latestReview := model.Update(firstResult)
	model = updated.(Model)
	if latestReview == nil || model.delivery.stage != deliveryNone ||
		model.mirrorReviewing[namespace] != latestGeneration {
		t.Fatalf("stale review overwrote state instead of scheduling latest: %+v", model)
	}
	updated, duplicateCommand := model.Update(firstResult)
	model = updated.(Model)
	if duplicateCommand != nil || model.delivery.stage != deliveryNone {
		t.Fatalf("duplicate stale review mutated current generation: %+v", model.delivery)
	}

	updated, _ = model.Update(commandResult(t, latestReview))
	model = updated.(Model)
	if model.delivery.stage != deliveryReviewed || model.delivery.generation != latestGeneration ||
		len(model.delivery.changes) != 1 || string(model.delivery.changes[0].NewContent) != "v2" ||
		model.active == nil || model.active.SessionKey() != assignment.SessionKey() {
		t.Fatalf("latest generation was not installed: %+v", model.delivery)
	}
}

func TestLiveWorkspaceWatchDuringConfirmCannotSuppressExactSend(t *testing.T) {
	model, _, namespace, events := liveWorkspaceGenerationFixture(t, false)
	client := model.client.(*fakeClient)

	updated, review := model.startMirrorReview()
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, review))
	model = updated.(Model)
	updated, _ = model.previewMirrorDelivery()
	model = updated.(Model)
	confirmedGeneration := model.delivery.generation
	confirmedEventID := model.delivery.eventID
	updated, confirm := model.confirmMirrorDelivery()
	model = updated.(Model)
	if confirm == nil || model.delivery.stage != deliveryConfirming {
		t.Fatalf("confirmation was not made exclusive: %+v", model.delivery)
	}
	confirmationResult := commandResult(t, confirm) // Delivery intent is durable here.

	if err := os.WriteFile(filepath.Join(model.mirrors[namespace].Dir(), "live.txt"), []byte("v2"), 0o600); err != nil {
		t.Fatal(err)
	}
	updated, _ = model.Update(mirrorWatchEvent{
		namespace: namespace, events: events, event: workmirror.WatchEvent{}, open: true,
	})
	model = updated.(Model)
	if model.delivery.stage != deliveryConfirming || model.delivery.eventID != confirmedEventID ||
		model.mirrorGeneration[namespace] <= confirmedGeneration {
		t.Fatalf("new save replaced an in-flight durable confirmation: %+v", model)
	}

	updated, send := model.Update(confirmationResult)
	model = updated.(Model)
	if send == nil || model.delivery.stage != deliverySending {
		t.Fatalf("durable confirmation result was suppressed: %+v", model.delivery)
	}
	client.sendErr = definitelyNotStored("outbox unavailable")
	updated, _ = model.Update(commandResult(t, send))
	model = updated.(Model)
	if model.delivery.stage != deliveryConfirmed || !model.mirrorDirty[namespace] || model.active == nil ||
		model.pending.kind != pendingDelivery || model.pending.durable ||
		model.pending.eventID != confirmedEventID {
		t.Fatalf("failed outbox lost exact confirmed event or queued save: %+v", model)
	}

	client.sendErr = nil
	updated, retry := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if retry == nil || model.delivery.stage != deliverySending {
		t.Fatalf("exact confirmed event was not retryable: %+v", model.delivery)
	}
	updated, _ = model.Update(commandResult(t, retry))
	model = updated.(Model)
	if len(client.events) != 2 || client.events[0].ID != confirmedEventID ||
		client.events[1].ID != confirmedEventID || model.delivery.stage != deliveryNone ||
		!model.mirrorDirty[namespace] {
		t.Fatalf("exact retry or newer dirty generation was lost: events=%+v model=%+v", client.events, model)
	}
	if got := client.events[1].ToolCalls[0].Input["content"]; got != "v1" {
		t.Fatalf("confirmed payload changed under a newer save: %#v", got)
	}
}

func TestRejectedScopeFinalizerBlocksManualAndAutomaticMirrorConfirmation(t *testing.T) {
	t.Run("manual preview enter", func(t *testing.T) {
		model, assignment, _, _ := liveWorkspaceGenerationFixture(t, false)
		updated, review := model.startMirrorReview()
		model = updated.(Model)
		updated, _ = model.Update(commandResult(t, review))
		model = updated.(Model)
		updated, _ = model.previewMirrorDelivery()
		model = updated.(Model)
		if model.delivery.stage != deliveryPreviewed {
			t.Fatalf("manual delivery was not previewed: %+v", model.delivery)
		}
		model.rejectionFinalizers["event-earlier-rejected"] = rejectionFinalizer{
			scope: rejectedDraftScopeKey(assignment), cleanupInFlight: true,
		}

		updated, command := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
		model = updated.(Model)
		if command != nil || model.pending.kind != pendingNone ||
			model.delivery.stage != deliveryPreviewed {
			t.Fatalf("manual confirmation crossed rejected-scope finalizer: %+v", model)
		}
	})

	t.Run("automatic review callback", func(t *testing.T) {
		model, assignment, namespace, _ := liveWorkspaceGenerationFixture(t, true)
		generation := model.requireMirrorReview(namespace)
		model.mirrorReviewing[namespace] = generation
		reviewed := commandResult(t, reviewMirrorAutomatically(
			model.mirrors[namespace], assignment, generation,
		))
		model.rejectionFinalizers["event-earlier-rejected"] = rejectionFinalizer{
			scope: rejectedDraftScopeKey(assignment), cleanupInFlight: true,
		}

		updated, command := model.Update(reviewed)
		model = updated.(Model)
		if command != nil || model.pending.kind != pendingNone ||
			model.delivery.stage != deliveryPreviewed {
			t.Fatalf("automatic confirmation crossed rejected-scope finalizer: %+v", model)
		}
	})
}

func liveWorkspaceGenerationFixture(
	t *testing.T,
	autoSend bool,
) (Model, completion.Assignment, string, chan workmirror.WatchEvent) {
	t.Helper()
	manager := newFilesystemMirrorManager(t.TempDir())
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "edit live.txt"}},
	}}})
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "live.txt"), []byte("v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	model := New(client, WithMirrorManager(manager), WithWorkspaceAutoSend(autoSend))
	model.active = &assignment
	model.rememberContext(assignment)
	model.mirrors[prepared.namespace] = prepared.workspace
	events := make(chan workmirror.WatchEvent)
	model.mirrorWatches[prepared.namespace] = mirrorWatchState{events: events}
	return model, assignment, prepared.namespace, events
}
