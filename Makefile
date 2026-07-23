.PHONY: all build test fault-test formal-check real-client-test real-opencode-web-test real-opencode-browser-test real-opencode-network-test real-codex-test real-claude-test real-container-client-test container-client-harness-test release-build release-semantics-test release-build-contract-test fmt fmt-check tidy-check vet check

# --cached includes tracked files deleted in the working tree. Filter them so
# cleanup branches do not make fmt-check invoke gofmt on paths that no longer
# exist (command substitution otherwise hides gofmt's non-zero status).
GO_FILES := $(shell git ls-files --cached --others --exclude-standard -- '*.go' | while IFS= read -r file; do test -f "$$file" && printf '%s\n' "$$file"; done)
FAULT_COUNT ?= 1
REAL_COUNT ?= 1
REAL_NETWORK_DROPS ?= 5
REAL_LLM_BASE_URL ?= http://127.0.0.1:23333
REAL_LLM_MODEL ?= dashscope:glm-5

all: check

build:
	go build ./...

test:
	go test ./...

fault-test:
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./internal/completion/gateway -run '^(TestCallerFiveTCPDisconnectsThenExactIdempotentRecovery|TestCodexResponsesFiveUnkeyedDisconnectsRecoverOneDerivedRequest|TestTransientHeartbeatFailureDoesNotAbandonLiveSession|TestContinueStreamingResponseStopsOnWriteOrFlushFailure|TestBeginStreamingReplayReturnsStartedCursorOnWriteFailure)$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./internal/workerclient -run '^(TestWorkerClientInitialTransientFailuresRecoverAfterFiveAttempts|TestWorkerClientInitialConnectionRefusedRetriesUntilGatewayStarts|TestWorkerClientReconnectsAndReceivesActiveAssignmentAgain|TestWorkerClientFiveFlapsPreserveAssignmentOutboxAndACKs|TestWorkerClientKeepaliveRecoversFromPeerThatStopsReading|TestWorkerClientReplaysTerminalEventAfterACKLoss|TestWorkerClientCredentialRotationReplaysPendingOutbox|TestExpiredSessionRejectsLateOutboxEventAndContinuesWithLiveWork|TestGatewayRestartWithLiveWorkerRecoversPartialCallerAndOfflineOutbox|TestThreePartyOutageRecoversExactlyOnce|TestWorkspaceToolLoopSurvivesThreePartyOutage)$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./llm -run '^TestServiceDurableFaultMatrix$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./agent -run '^TestAgentDurableFaultMatrix$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./humantest -run '^(TestAgentWorkerJournalRecoverySuiteAgainstMemoryImage|TestLLMWorkerJournalRecoverySuiteAgainstMemoryImage|TestMemoryAgentWorkerJournalAbandonRecoversCommittedImage|TestMemoryLLMWorkerJournalAbandonRecoversCommittedImage)$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./agent/sqlite ./llm/sqlite -run '^TestStoreRecoversCommittedTransactionAfterAbruptProcessExit$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./examples/custom-framework/customstore -run '^TestFileStoreRecoversCommittedSnapshotAfterAbruptProcessExit$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./agent/workerws/sqlite ./llm/workerws/sqlite -run '^(TestJournalRecoveryFaultMatrix|TestJournalRecoversCommittedStateAfterAbruptProcessExit)$$'

formal-check:
	./formal/run-checks.sh

real-opencode-web-test:
	HUMAN_REAL_OPENCODE_E2E=1 go test -count=$(REAL_COUNT) -timeout=5m ./local -run '^TestRealOpenCodeLocalPublicStack$$' -v
	HUMAN_REAL_OPENCODE_E2E=1 go test -count=$(REAL_COUNT) -timeout=5m ./web -run '^TestRealOpenCode' -v

real-opencode-browser-test:
	HUMAN_REAL_OPENCODE_BROWSER_E2E=1 go test -count=$(REAL_COUNT) -timeout=5m ./local -run '^TestRealOpenCodeWorkspaceBrowserFinal$$' -v

real-opencode-network-test:
	HUMAN_REAL_OPENCODE_NETWORK_E2E=1 HUMAN_REAL_OPENCODE_NETWORK_DROPS=$(REAL_NETWORK_DROPS) go test -count=$(REAL_COUNT) -timeout=8m ./local -run '^TestRealOpenCodeRecoversAcrossNetworkFaultMatrix$$' -v

real-codex-test:
	HUMAN_REAL_CODEX_E2E=1 go test -count=$(REAL_COUNT) -timeout=5m ./local -run '^TestRealCodexLocalPublicStackToolLoop$$' -v

real-claude-test:
	HUMAN_REAL_CLAUDE_E2E=1 go test -count=$(REAL_COUNT) -timeout=5m ./web -run '^TestRealClaudeCodeWebBasicLoop$$' -v

real-client-test: real-opencode-web-test real-codex-test real-claude-test

real-container-client-test:
	@test -n "$$HUMAN_TEST_LLM_API_KEY" || (echo "HUMAN_TEST_LLM_API_KEY is required" >&2; exit 2)
	HUMAN_TESTCONTAINERS_E2E=1 HUMAN_TEST_LLM_FAKE=0 \
	HUMAN_TEST_LLM_BASE_URL="$(REAL_LLM_BASE_URL)" \
	HUMAN_TEST_LLM_MODEL="$(REAL_LLM_MODEL)" \
	go -C tests/container-clients test -count=$(REAL_COUNT) -timeout=25m . -run '^TestContainer(Protocols|AgentCLIs)ViaLLMWebHuman$$' -v

container-client-harness-test:
	HUMAN_TESTCONTAINERS_E2E=1 HUMAN_TEST_LLM_FAKE=1 \
	go -C tests/container-clients test -count=$(REAL_COUNT) -timeout=25m . -run '^TestContainer(Protocols|AgentCLIs)ViaLLMWebHuman$$' -v

release-build:
	test -n "$(VERSION)"
	VERSION="$(VERSION)" COMMIT="$$(git rev-parse HEAD)" BUILD_DATE="$$(git show -s --format=%cI HEAD)" ./scripts/build-release.sh

release-semantics-test:
	./scripts/github-release-flags_test.sh

release-build-contract-test:
	./scripts/build-release-contract-test.sh

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	test -z "$$(gofmt -l $(GO_FILES))"

tidy-check:
	go mod tidy -diff

vet:
	go vet ./...

check: fmt-check tidy-check build test vet release-semantics-test release-build-contract-test
