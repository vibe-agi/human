package tui

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
)

func TestIdleModelDoesNotStartOrRescheduleSpinner(t *testing.T) {
	model := New(newFakeClient())
	initial := model.Init()()
	batch, ok := initial.(tea.BatchMsg)
	if !ok {
		t.Fatalf("Init returned %T, want the network/theme batch", initial)
	}
	if len(batch) != 2 {
		t.Fatalf("idle Init scheduled %d commands, want only network and theme", len(batch))
	}

	frame := model.ui.spinner.View()
	updated, command := model.Update(model.ui.spinner.Tick())
	model = updated.(Model)
	if command != nil {
		t.Fatal("idle spinner tick scheduled another frame")
	}
	if got := model.ui.spinner.View(); got != frame {
		t.Fatalf("idle spinner advanced from %q to %q", frame, got)
	}
}

func TestAnimationStartsForSendAndStopsAfterAcknowledgement(t *testing.T) {
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	model.assignments = append(model.assignments, assignment)
	updated, command := model.Update(press("a", 'a'))
	model = updated.(Model)
	if command == nil || !model.animationActive() {
		t.Fatal("staged accept did not start the sending animation")
	}
	message := command()
	batch, ok := message.(tea.BatchMsg)
	if !ok {
		t.Fatalf("sending command returned %T, want operation plus animation batch", message)
	}
	var acknowledgement tea.Msg
	sawTick := false
	for _, child := range batch {
		candidate := child()
		if _, ok := candidate.(spinner.TickMsg); ok {
			sawTick = true
			continue
		}
		acknowledgement = candidate
	}
	if !sawTick || acknowledgement == nil {
		t.Fatalf("sending batch omitted tick=%t or acknowledgement=%T", sawTick, acknowledgement)
	}
	updated, command = model.Update(acknowledgement)
	model = updated.(Model)
	if command != nil || model.animationActive() || model.pending.kind != pendingNone {
		t.Fatalf("acknowledged send kept animating: pending=%v command=%v", model.pending.kind, command != nil)
	}
}

func TestActiveSpinnerTickReschedulesOnlyWhileAnimating(t *testing.T) {
	model := New(newFakeClient())
	model.connection = connectionReconnecting
	before := model.ui.spinner.View()
	updated, command := model.Update(model.ui.spinner.Tick())
	model = updated.(Model)
	if command == nil {
		t.Fatal("reconnecting spinner did not schedule its next frame")
	}
	if got := model.ui.spinner.View(); got == before {
		t.Fatalf("reconnecting spinner did not advance from %q", before)
	}

	model.connection = connectionConnected
	updated, command = model.Update(model.ui.spinner.Tick())
	model = updated.(Model)
	if command != nil {
		t.Fatal("spinner kept scheduling after reconnect completed")
	}
}

func TestChatViewportCacheSkipsEditorOnlyUpdatesAndInvalidatesContent(t *testing.T) {
	client := newFakeClient()
	assignment := testAssignment()
	model := New(client)
	model.active = &assignment
	model.focus = focusReply
	model.invalidateChat()
	model.resizeUI()

	// A sentinel makes an otherwise invisible SetContent call observable.
	model.ui.chat.SetContent("cache-sentinel")
	updated, _ := model.Update(press("x", 'x'))
	model = updated.(Model)
	if !strings.Contains(model.ui.chat.View(), "cache-sentinel") {
		t.Fatal("reply-only update rebuilt the unchanged chat viewport")
	}

	updated, send := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if send == nil {
		t.Fatal("reply did not produce a send command")
	}
	if transcript := model.ui.chat.View(); strings.Contains(transcript, "cache-sentinel") ||
		!strings.Contains(transcript, "x") {
		t.Fatalf("reply content did not invalidate the chat cache:\n%s", transcript)
	}
}

func TestNetworkReplayAndDetailModeInvalidateChatViewport(t *testing.T) {
	assignment := testAssignment()
	model := New(newFakeClient())
	model.active = &assignment
	model.rememberContext(assignment)
	model.resizeUI()

	model.ui.chat.SetContent("stale-network-cache")
	replayed := assignment
	updated, _ := model.Update(networkMessage{Assignment: &replayed})
	model = updated.(Model)
	if strings.Contains(model.ui.chat.View(), "stale-network-cache") ||
		!strings.Contains(model.ui.chat.View(), "help") {
		t.Fatalf("network replay left stale transcript:\n%s", model.ui.chat.View())
	}

	model.focus = focusTasks
	updated, _ = model.Update(press("v", 'v'))
	model = updated.(Model)
	if !model.detailMode || !strings.Contains(model.ui.chat.View(), "Request (full") {
		t.Fatalf("detail mode left stale readable transcript:\n%s", model.ui.chat.View())
	}
}

func TestTaskAndAdvancedToolCallsInvalidateChatViewport(t *testing.T) {
	assignment := testAssignment()
	model := New(newFakeClient())
	model.active = &assignment
	model.rememberContext(assignment)
	model.resizeUI()
	model.ui.chat.SetContent("stale-tool-cache")

	model.appendLocalToolCall(assignment, completion.ToolCall{
		ID: "tool-1", Name: "update_plan", Input: map[string]any{"plan": []any{}},
	})
	model.resizeUI()
	transcript := model.ui.chat.View()
	if strings.Contains(transcript, "stale-tool-cache") || !strings.Contains(transcript, "update_plan") {
		t.Fatalf("tool/task update left stale transcript:\n%s", transcript)
	}
}

func TestViewDoesNotMutateChatCache(t *testing.T) {
	model := New(newFakeClient())
	if !model.ui.chatDirty {
		t.Fatal("new model should require its first chat render")
	}
	beforeWidth, beforeHeight := model.ui.chatWidth, model.ui.chatHeight
	_ = model.View()
	if !model.ui.chatDirty || model.ui.chatWidth != beforeWidth || model.ui.chatHeight != beforeHeight {
		t.Fatal("View mutated the model's chat cache")
	}
}
