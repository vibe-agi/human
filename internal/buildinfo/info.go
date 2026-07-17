// Package buildinfo exposes release metadata injected by the build pipeline.
package buildinfo

import "runtime"

// These variables are intentionally writable only so release builds can set
// them with -ldflags -X. Development builds retain explicit, diagnosable
// values instead of pretending to be a tagged release.
var (
	Version = "0.1.0-dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Info is the machine-readable build identity printed by `human version` and
// consumed by diagnostics and release smoke tests.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// Current returns a snapshot so tests and embedders cannot mutate the package
// variables through a shared object.
func Current() Info {
	return Info{
		Version: Version, Commit: Commit, BuildDate: Date,
		GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
	}
}
