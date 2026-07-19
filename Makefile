.PHONY: all build test fault-test formal-check real-opencode-tui-test real-opencode-network-test release-build release-semantics-test release-build-contract-test fmt fmt-check tidy-check vet check

GO_FILES := $(shell git ls-files --cached --others --exclude-standard -- '*.go')
FAULT_COUNT ?= 1
REAL_COUNT ?= 1
REAL_NETWORK_DROPS ?= 5

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

real-opencode-tui-test:
	HUMAN_REAL_OPENCODE_TUI_E2E=1 go test -count=$(REAL_COUNT) -timeout=2m ./local -run '^TestRealOpenCodeTUIWorkspaceLoop$$' -v

real-opencode-network-test:
	HUMAN_REAL_OPENCODE_NETWORK_E2E=1 HUMAN_REAL_OPENCODE_NETWORK_DROPS=$(REAL_NETWORK_DROPS) go test -count=$(REAL_COUNT) -timeout=8m ./local -run '^TestRealOpenCodeRecoversAcrossNetworkFaultMatrix$$' -v

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
	go mod tidy
	git diff --exit-code -- go.mod go.sum

vet:
	go vet ./...

check: fmt-check tidy-check build test vet release-semantics-test release-build-contract-test
