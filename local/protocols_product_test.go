package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLocalWebHumanCompletesEveryBuiltinProtocol is the non-optional product
// door for the three advertised model APIs. The caller and the human both use
// HTTP, and every protocol is exercised in aggregate and streaming mode.
func TestLocalWebHumanCompletesEveryBuiltinProtocol(t *testing.T) {
	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	workspace := filepath.Join(root, "workspace")
	for _, directory := range []string{privateRoot, workspace} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	instance, err := Open(t.Context(), Config{
		HumanWorkspaceRoot: workspace,
		Public: PublicStackConfig{
			DatabasePath: filepath.Join(privateRoot, "store.db"),
			MaxPending:   10 * time.Second,
		},
		ListenAddress:       "127.0.0.1:0",
		WebListenAddress:    "127.0.0.1:0",
		WebStatePath:        filepath.Join(privateRoot, "workerkit-state.db"),
		WebDisableAutoTitle: true,
		ShutdownTimeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	client := &http.Client{Timeout: 10 * time.Second}
	operator := webAPIForLocal(t, instance, client)
	tests := []struct {
		name, path string
		body       func(bool) string
		anthropic  bool
	}{
		{
			name: "openai-chat", path: "/v1/chat/completions",
			body: func(stream bool) string {
				return fmt.Sprintf(`{"model":"human-expert","stream":%t,"messages":[{"role":"user","content":"complete through the web human"}]}`, stream)
			},
		},
		{
			name: "openai-responses", path: "/v1/responses",
			body: func(stream bool) string {
				return fmt.Sprintf(`{"model":"human-expert","stream":%t,"input":"complete through the web human"}`, stream)
			},
		},
		{
			name: "anthropic-messages", path: "/v1/messages", anthropic: true,
			body: func(stream bool) string {
				return fmt.Sprintf(`{"model":"human-expert","max_tokens":256,"stream":%t,"messages":[{"role":"user","content":"complete through the web human"}]}`, stream)
			},
		},
	}
	for _, test := range tests {
		for _, stream := range []bool{false, true} {
			name := fmt.Sprintf("%s/stream=%t", test.name, stream)
			t.Run(name, func(t *testing.T) {
				marker := "HUMAN-WEB-" + strings.ToUpper(strings.ReplaceAll(name, "/", "-"))
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				result := make(chan struct {
					status int
					body   string
					err    error
				}, 1)
				go func() {
					request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, instance.BaseURL()+test.path, strings.NewReader(test.body(stream)))
					if requestErr != nil {
						result <- struct {
							status int
							body   string
							err    error
						}{err: requestErr}
						return
					}
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set("Idempotency-Key", "product-"+strings.NewReplacer("/", "-", "=", "-").Replace(name))
					if test.anthropic {
						request.Header.Set("X-Api-Key", instance.CallerToken())
						request.Header.Set("Anthropic-Version", "2023-06-01")
					} else {
						request.Header.Set("Authorization", "Bearer "+instance.CallerToken())
					}
					response, doErr := client.Do(request)
					if doErr != nil {
						result <- struct {
							status int
							body   string
							err    error
						}{err: doErr}
						return
					}
					payload, readErr := io.ReadAll(response.Body)
					closeErr := response.Body.Close()
					result <- struct {
						status int
						body   string
						err    error
					}{response.StatusCode, string(payload), errors.Join(readErr, closeErr)}
				}()

				if err := completeNextInboxViaWeb(ctx, operator, marker); err != nil {
					t.Fatal(err)
				}
				call := <-result
				if call.status != http.StatusOK || !strings.Contains(call.body, marker) {
					t.Fatalf("status=%d err=%v body=%s", call.status, call.err, call.body)
				}
			})
		}
	}
}

func completeNextInboxViaWeb(ctx context.Context, api localWebAPI, marker string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		state, status, err := api.call(ctx, http.MethodGet, "/api/state", nil)
		if err != nil || status != http.StatusOK {
			continue
		}
		inbox, _ := state["inbox"].([]any)
		if len(inbox) == 0 {
			continue
		}
		item, _ := inbox[0].(map[string]any)
		accepted, status, err := api.call(ctx, http.MethodPost, "/api/accept", map[string]any{"delivery": item["delivery"]})
		if err != nil || status != http.StatusOK {
			return fmt.Errorf("accept web assignment: status=%d err=%v", status, err)
		}
		key, _ := accepted["key"].(map[string]any)
		_, status, err = api.call(ctx, http.MethodPost, "/api/final", map[string]any{
			"caller": key["caller"], "task_id": key["task_id"], "text": marker,
		})
		if err != nil || status != http.StatusOK {
			return fmt.Errorf("complete web assignment: status=%d err=%v", status, err)
		}
		return nil
	}
}
