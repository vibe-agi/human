package buildinfo

import (
	"runtime"
	"testing"
)

func TestCurrentIncludesRuntimeAndBuildIdentity(t *testing.T) {
	info := Current()
	if info.Version == "" || info.Commit == "" || info.BuildDate == "" {
		t.Fatalf("build identity is incomplete: %+v", info)
	}
	if info.GoVersion != runtime.Version() || info.OS != runtime.GOOS || info.Arch != runtime.GOARCH {
		t.Fatalf("runtime identity = %+v", info)
	}
}

func TestCurrentReturnsAnIndependentSnapshot(t *testing.T) {
	first := Current()
	first.Version = "mutated"
	first.Commit = "mutated"
	first.BuildDate = "mutated"

	second := Current()
	if second.Version != Version || second.Commit != Commit || second.BuildDate != Date {
		t.Fatalf("mutating returned build info changed package identity: %+v", second)
	}
}
