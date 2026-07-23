package local

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOpenWebModeServesBrowserHumanSide proves the reference product path: the
// public service serves the caller while workerkit + web use an in-process
// worker connection to deliver a human response.
func TestOpenWebModeServesBrowserHumanSide(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		Public:             PublicStackConfig{DatabasePath: filepath.Join(root, "store.db")},
		HumanWorkspaceRoot: filepath.Join(root, "mirror"),
		ListenAddress:      "127.0.0.1:0",
		WebListenAddress:   "127.0.0.1:0",
		WebStatePath:       filepath.Join(privateRoot, "workerkit-state.db"),
		CallerSubject:      "test-caller",
		WorkerSubject:      "test-worker",
	}
	instance, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	if instance.WebWorker() == nil {
		t.Fatal("web mode did not compose the workerkit human side")
	}
	webURL := instance.WebURL()
	if !strings.HasPrefix(webURL, "http://127.0.0.1:") || !strings.Contains(webURL, "?token=") {
		t.Fatalf("web URL = %q", webURL)
	}
	base := webURL[:strings.Index(webURL, "/?token=")]
	token := webURL[strings.Index(webURL, "?token=")+len("?token="):]

	webJSON := func(method, path string, body any, want int) map[string]any {
		var reader io.Reader
		if body != nil {
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			reader = bytes.NewReader(encoded)
		}
		request, err := http.NewRequest(method, base+path, reader)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var decoded map[string]any
		_ = json.NewDecoder(response.Body).Decode(&decoded)
		if response.StatusCode != want {
			t.Fatalf("%s %s = %d (%v), want %d", method, path, response.StatusCode, decoded, want)
		}
		return decoded
	}

	// The scripted human: accept the first inbox item and deliver a final.
	const finalAnswer = "LOCAL-WEB-MODE-FINAL: bridged loop complete"
	operatorDone := make(chan struct{})
	go func() {
		defer close(operatorDone)
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			state := webJSON(http.MethodGet, "/api/state", nil, http.StatusOK)
			inbox, _ := state["inbox"].([]any)
			if len(inbox) == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			item, _ := inbox[0].(map[string]any)
			accepted := webJSON(http.MethodPost, "/api/accept",
				map[string]any{"delivery": item["delivery"]}, http.StatusOK)
			key, _ := accepted["key"].(map[string]any)
			webJSON(http.MethodPost, "/api/reply", map[string]any{
				"caller": key["caller"], "task_id": key["task_id"], "text": "looking",
			}, http.StatusOK)
			webJSON(http.MethodPost, "/api/final", map[string]any{
				"caller": key["caller"], "task_id": key["task_id"], "text": finalAnswer,
			}, http.StatusOK)
			return
		}
	}()

	// A tool-less request is auto-answered with a derived title and never
	// reaches the human inbox (OpenCode title-generation behavior).
	titleContext, cancelTitle := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelTitle()
	titleBody := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"summarize this conversation title"}]}`)
	titleRequest, err := http.NewRequestWithContext(
		titleContext, http.MethodPost, instance.BaseURL()+"/v1/chat/completions", bytes.NewReader(titleBody),
	)
	if err != nil {
		t.Fatal(err)
	}
	titleRequest.Header.Set("Authorization", "Bearer "+instance.CallerToken())
	titleRequest.Header.Set("Content-Type", "application/json")
	titleRequest.Header.Set("Idempotency-Key", "web-mode-title-1")
	titleResponse, err := http.DefaultClient.Do(titleRequest)
	if err != nil {
		t.Fatal(err)
	}
	titleStream, err := io.ReadAll(titleResponse.Body)
	_ = titleResponse.Body.Close()
	if err != nil || titleResponse.StatusCode != http.StatusOK ||
		!strings.Contains(string(titleStream), "summarize this conversation title") {
		t.Fatalf("auto-title response = %d, %v:\n%s", titleResponse.StatusCode, err, titleStream)
	}

	// The caller: one streamed, tool-declaring chat completion (a real turn).
	requestContext, cancelRequest := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelRequest()
	body := []byte(`{"model":"human-expert","stream":true,` +
		`"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],` +
		`"messages":[{"role":"user","content":"please answer"}]}`)
	chatRequest, err := http.NewRequestWithContext(
		requestContext, http.MethodPost, instance.BaseURL()+"/v1/chat/completions", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	chatRequest.Header.Set("Authorization", "Bearer "+instance.CallerToken())
	chatRequest.Header.Set("Content-Type", "application/json")
	chatRequest.Header.Set("Idempotency-Key", "web-mode-turn-1")
	chatResponse, err := http.DefaultClient.Do(chatRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer chatResponse.Body.Close()
	if chatResponse.StatusCode != http.StatusOK {
		failure, _ := io.ReadAll(chatResponse.Body)
		t.Fatalf("chat status = %d, body = %s", chatResponse.StatusCode, failure)
	}
	stream, err := io.ReadAll(chatResponse.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stream), "LOCAL-WEB-MODE-FINAL") {
		t.Fatalf("caller stream lacks the web-delivered final:\n%s", stream)
	}
	<-operatorDone

	// The web state survives in its own durable database.
	if _, err := os.Stat(config.WebStatePath); err != nil {
		t.Fatalf("web state database missing: %v", err)
	}
	if err := instance.Close(); err != nil {
		t.Fatalf("close web-mode local: %v", err)
	}
	// The web listener is down after Close.
	client := http.Client{Timeout: time.Second}
	if response, err := client.Get(base + "/api/state"); err == nil {
		_ = response.Body.Close()
		t.Fatalf("web listener still accepted requests after Close: %s", response.Status)
	}
}
