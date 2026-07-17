package humancmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vibe-agi/human/internal/buildinfo"
)

func TestVersionCommandPrintsCompleteHumanReadableIdentity(t *testing.T) {
	t.Parallel()
	command := newVersionCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	info := buildinfo.Current()
	want := "human " + info.Version + "\n" +
		"commit: " + info.Commit + "\n" +
		"built: " + info.BuildDate + "\n" +
		"runtime: " + info.GoVersion + "\n" +
		"platform: " + info.OS + "/" + info.Arch + "\n"
	if got := output.String(); got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}

func TestVersionCommandPrintsStableJSON(t *testing.T) {
	t.Parallel()
	command := newVersionCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--json"})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got buildinfo.Info
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("decode version JSON %q: %v", output.String(), err)
	}
	if want := buildinfo.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON build info = %+v, want %+v", got, want)
	}
	wantJSON, err := json.Marshal(buildinfo.Current())
	if err != nil {
		t.Fatal(err)
	}
	wantJSON = append(wantJSON, '\n')
	if !bytes.Equal(output.Bytes(), wantJSON) {
		t.Fatalf("version JSON = %q, want stable encoding %q", output.Bytes(), wantJSON)
	}
	wantFields := []string{"version", "commit", "build_date", "go_version", "os", "arch"}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &object); err != nil {
		t.Fatal(err)
	}
	if len(object) != len(wantFields) {
		t.Fatalf("JSON fields = %v", object)
	}
	for _, field := range wantFields {
		if _, ok := object[field]; !ok {
			t.Fatalf("version JSON omitted %q: %s", field, output.String())
		}
	}
}

func TestVersionCommandRejectsArguments(t *testing.T) {
	t.Parallel()
	command := newVersionCommand()
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	command.SetArgs([]string{"unexpected"})
	if err := command.ExecuteContext(context.Background()); err == nil {
		t.Fatal("version command accepted a positional argument")
	}
}

func TestVersionCommandDoesNotDependOnConfiguration(t *testing.T) {
	t.Parallel()
	configPath := filepath.Join(t.TempDir(), "broken.toml")
	if err := os.WriteFile(configPath, []byte("not = [valid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := New()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--config", configPath, "version", "--json"})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("version command loaded an unrelated broken config: %v", err)
	}
	var got buildinfo.Info
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("decode version JSON %q: %v", output.String(), err)
	}
}

func TestWriteHumanVersionPropagatesWriterFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("write failed")
	err := writeHumanVersion(failingVersionWriter{err: want}, buildinfo.Current())
	if !errors.Is(err, want) {
		t.Fatalf("write error = %v, want %v", err, want)
	}
}

type failingVersionWriter struct {
	err error
}

func (writer failingVersionWriter) Write([]byte) (int, error) {
	return 0, writer.err
}

var _ io.Writer = failingVersionWriter{}
