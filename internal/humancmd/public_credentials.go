package humancmd

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

const publicCredentialVersion = 1

// publicCredentialFile persists the single loopback caller token for the public
// llm.Service stack. The in-process worker needs no token, so there is no worker
// credential and no two-phase rotation journal — just one secret the OpenCode
// provider config points at, kept 0600 and rewritten atomically.
type publicCredentialFile struct {
	Version     int    `json:"version"`
	CallerToken string `json:"caller_token"`
}

// ensurePublicCallerToken returns the persisted caller token, minting and
// persisting a fresh one when none exists or when a reset is requested. The
// token is stable across restarts so the OpenCode provider config keeps working.
func ensurePublicCallerToken(path string, reset bool) (string, error) {
	if !reset {
		token, found, err := readPublicCredentials(path)
		if err != nil {
			return "", err
		}
		if found {
			return token, nil
		}
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("allocate local caller token: %w", err)
	}
	token := hex.EncodeToString(raw)
	if err := writePublicCredentials(path, token); err != nil {
		return "", err
	}
	return token, nil
}

// readPublicCredentials reads the caller token, failing closed on a file that is
// a symlink, group/other-readable, oversized, or structurally invalid.
func readPublicCredentials(path string) (string, bool, error) {
	const maxCredentialBytes = 64 << 10
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("inspect local credentials: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", false, errors.New("local credential path is not a regular file")
	}
	if info.Size() > maxCredentialBytes {
		return "", false, errors.New("local credential file exceeds the 64 KiB limit")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", false, fmt.Errorf("local credential file %s must not be accessible by group or others", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", false, fmt.Errorf("open local credentials: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return "", false, fmt.Errorf("inspect opened local credentials: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return "", false, errors.New("local credential file changed while opening")
	}
	if runtime.GOOS != "windows" && opened.Mode().Perm()&0o077 != 0 {
		return "", false, fmt.Errorf("local credential file %s must not be accessible by group or others", path)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxCredentialBytes+1))
	if err != nil {
		return "", false, fmt.Errorf("read local credentials: %w", err)
	}
	if len(payload) > maxCredentialBytes {
		return "", false, errors.New("local credential file exceeds the 64 KiB limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var credentials publicCredentialFile
	if err := decoder.Decode(&credentials); err != nil {
		return "", false, fmt.Errorf("decode local credentials: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return "", false, fmt.Errorf("decode local credentials: trailing data: %w", err)
	}
	if credentials.Version != publicCredentialVersion || credentials.CallerToken == "" {
		return "", false, errors.New("local credential file is malformed")
	}
	return credentials.CallerToken, true, nil
}

// writePublicCredentials writes the caller token atomically at 0600 into a 0700
// directory.
func writePublicCredentials(path, token string) error {
	if token == "" {
		return errors.New("refusing to persist an empty caller token")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	if info, err := os.Stat(directory); err != nil {
		return err
	} else if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("local credential directory %s must not be accessible by group or others", directory)
	}
	temporary, err := os.CreateTemp(directory, ".local-caller-token-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(publicCredentialFile{Version: publicCredentialVersion, CallerToken: token}); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		directoryHandle, err := os.Open(directory)
		if err != nil {
			return err
		}
		syncErr := directoryHandle.Sync()
		closeErr := directoryHandle.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
