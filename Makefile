.PHONY: all build test fault-test real-opencode-tui-test real-opencode-network-test release-build fmt fmt-check tidy-check vet check

GO_FILES := $(shell git ls-files '*.go')
FAULT_COUNT ?= 1
REAL_COUNT ?= 1

all: check

build:
	go build ./...

test:
	go test ./...

fault-test:
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./internal/completion/gateway -run '^(TestCallerFiveTCPDisconnectsThenExactIdempotentRecovery|TestCodexResponsesFiveUnkeyedDisconnectsRecoverOneDerivedRequest|TestTransientHeartbeatFailureDoesNotAbandonLiveSession|TestContinueStreamingResponseStopsOnWriteOrFlushFailure|TestBeginStreamingReplayReturnsStartedCursorOnWriteFailure)$$'
	go test -race -count=$(FAULT_COUNT) -timeout=5m ./internal/workerclient -run '^(TestWorkerClientInitialTransientFailuresRecoverAfterFiveAttempts|TestWorkerClientInitialConnectionRefusedRetriesUntilGatewayStarts|TestWorkerClientReconnectsAndReceivesActiveAssignmentAgain|TestWorkerClientFiveFlapsPreserveAssignmentOutboxAndACKs|TestWorkerClientKeepaliveRecoversFromPeerThatStopsReading|TestWorkerClientReplaysTerminalEventAfterACKLoss|TestWorkerClientOutboxSurvivesClientProcessReopen|TestExpiredSessionRejectsLateOutboxEventAndContinuesWithLiveWork|TestGatewayRestartWithLiveWorkerRecoversPartialCallerAndOfflineOutbox|TestThreePartyOutageRecoversExactlyOnce|TestWorkspaceToolLoopSurvivesThreePartyOutage)$$'

real-opencode-tui-test:
	HUMAN_REAL_OPENCODE_TUI_E2E=1 go test -count=$(REAL_COUNT) -timeout=2m ./local -run '^TestRealOpenCodeTUIWorkspaceLoop$$' -v

real-opencode-network-test:
	HUMAN_REAL_OPENCODE_NETWORK_E2E=1 go test -count=$(REAL_COUNT) -timeout=2m ./local -run '^TestRealOpenCodeRecoversAcrossNetworkFaultMatrix$$' -v

release-build:
	test -n "$(VERSION)"
	VERSION="$(VERSION)" COMMIT="$$(git rev-parse HEAD)" ./scripts/build-release.sh

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	test -z "$$(gofmt -l $(GO_FILES))"

tidy-check:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

vet:
	go vet ./...

check: fmt-check tidy-check build test vet
