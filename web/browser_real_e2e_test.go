package web_test

// The browser real door: a REAL browser (Playwright driving system Chrome)
// operates the embedded web UI while an OpenAI-compatible LLM simulates the
// human expert's judgement. The caller side drives the embedded public kernel
// directly, so the full path under test is:
//
//	caller → llm.Service → workerkit → web UI in Chrome → simulated human (LLM)
//	       ← final answer typed and delivered through the real page
//
//	HUMAN_REAL_WEB_BROWSER_E2E=1 CHERRY_API_KEY=... \
//	  go test ./web -run TestRealBrowserWebDoorWithSimulatedHuman -count=1

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"net/http/httptest"
)

func TestRealBrowserWebDoorWithSimulatedHuman(t *testing.T) {
	if os.Getenv("HUMAN_REAL_WEB_BROWSER_E2E") != "1" {
		t.Skip("set HUMAN_REAL_WEB_BROWSER_E2E=1 to run the Playwright browser door")
	}
	nodeBinary, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	cherryKey := os.Getenv("CHERRY_API_KEY")
	if cherryKey == "" {
		t.Skip("set CHERRY_API_KEY for the simulated-human LLM")
	}
	cherryURL := os.Getenv("CHERRY_API_URL")
	if cherryURL == "" {
		cherryURL = "http://127.0.0.1:23333/v1"
	}
	cherryModel := os.Getenv("CHERRY_MODEL")
	if cherryModel == "" {
		cherryModel = "dashscope:glm-5"
	}

	// --- Embedded stack: kernel → workerkit → web.
	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "browser-door",
		Store:        framework.Borrow[llm.Store](store),
		Codecs: []llm.CodecRegistration{{
			Codec: integrationBrowserCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}},
		Admission: llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	})
	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-browser",
	})
	if err != nil {
		t.Fatal(err)
	}
	stateStore, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{
		Wire: workerkit.WrapConnection(connection), State: stateStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	webServer, err := web.New(web.Config{Worker: worker, SessionToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	listener := httptest.NewServer(webServer)
	t.Cleanup(func() {
		listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = webServer.Shutdown(ctx)
		_ = worker.Shutdown(ctx)
	})

	// --- Caller: one streamed chat completion waiting for the human.
	const finalToken = "HUMAN-BROWSER-DOOR"
	request := llm.Request{
		Model: "human", Stream: true,
		Messages: []llm.Message{{Role: llm.RoleUser, Blocks: []llm.Block{{
			Type: llm.BlockText,
			Text: "How should I safely roll back a bad production deploy? Please answer through the human console.",
		}}}},
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-browser", IdempotencyKey: "browser-turn-1",
		CodecID: browserCodecID, Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}

	// --- Browser driver: install the pinned Playwright dependency once, then
	// let Chrome operate the page with the LLM as the human's judgement.
	e2eDir, err := filepath.Abs(filepath.Join("e2e"))
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(filepath.Join(e2eDir, "node_modules", "playwright")); statErr != nil {
		install := exec.Command("npm", "install", "--no-audit", "--no-fund")
		install.Dir = e2eDir
		if output, installErr := install.CombinedOutput(); installErr != nil {
			t.Fatalf("npm install: %v\n%s", installErr, output)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	driver := exec.CommandContext(ctx, nodeBinary, "driver.mjs")
	driver.Dir = e2eDir
	driver.Env = append(os.Environ(),
		"WEB_URL="+listener.URL,
		"WEB_TOKEN="+testToken,
		"CHERRY_URL="+cherryURL,
		"CHERRY_KEY="+cherryKey,
		"CHERRY_MODEL="+cherryModel,
		"FINAL_TOKEN="+finalToken,
	)
	output, driverErr := driver.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("browser driver timed out: %v\n%s", ctx.Err(), output)
	}
	if driverErr != nil {
		t.Fatalf("browser driver failed: %v\n%s", driverErr, output)
	}
	if !strings.Contains(string(output), "browser-door-ok") {
		t.Fatalf("browser driver did not confirm: %s", output)
	}

	// --- The caller must observe the browser-delivered final answer.
	deadline := time.Now().Add(30 * time.Second)
	for {
		page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
			CallerID: "caller-browser", IdempotencyKey: "browser-turn-1",
			RequestDigest: admission.RequestDigest,
		})
		if err != nil {
			t.Fatal(err)
		}
		if page.Complete {
			var wire strings.Builder
			for _, event := range page.Events {
				wire.Write(event.Data)
			}
			if !strings.Contains(wire.String(), finalToken) {
				t.Fatalf("caller response lacks the simulated human's token:\n%s", wire.String())
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("caller response never completed: %+v", page)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

const browserCodecID llm.CodecID = "workerkit.browser"

type integrationBrowserCodec struct{}

func (integrationBrowserCodec) Description() llm.CodecDescription {
	return llm.CodecDescription{
		Contract: framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
		ID:       browserCodecID, Version: "1",
		Fingerprint: llm.Fingerprint([]byte("workerkit-browser-v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes: 1 << 20, MaxStreamFrameBytes: 1 << 20,
			MaxStreamFramesPerStep: 16, MaxAggregateBytes: 1 << 20,
			MaxAdmissionErrorBytes: 1 << 20,
		},
		OverloadedStatus: 503,
	}
}

func (integrationBrowserCodec) Decode(body []byte) (llm.Request, error) {
	var request llm.Request
	err := json.Unmarshal(body, &request)
	return request, err
}

func (integrationBrowserCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &browserEncoder{}, nil
}

func (integrationBrowserCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	return integrationBrowserCodec{}.NewStream(session)
}

func (integrationBrowserCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return json.Marshal(failure)
}

type browserEncoder struct{}

func (*browserEncoder) Start() ([][]byte, error) { return [][]byte{[]byte("start\n")}, nil }

func (*browserEncoder) Encode(event llm.Event, _ llm.EventSeed) ([][]byte, bool, error) {
	return [][]byte{[]byte(string(event.Type) + ":" + event.Text + "\n")}, event.EndsResponse(), nil
}
