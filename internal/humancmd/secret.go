package humancmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"unicode"
)

const maxSecretBytes = 64 << 10

func resolveSecret(environmentName, filePath, description string) (string, error) {
	environmentValue, environmentSet := os.LookupEnv(environmentName)
	filePath = strings.TrimSpace(filePath)
	if environmentSet && filePath != "" {
		return "", fmt.Errorf("%s is configured twice; use either %s or a token file, not both", description, environmentName)
	}
	if environmentSet {
		return validateSecret(environmentValue, description+" from "+environmentName)
	}
	if filePath == "" {
		return "", fmt.Errorf("%s is required; set %s or provide a private token file", description, environmentName)
	}

	resolvedPath, err := resolvePrivatePath(filePath, description+" file")
	if err != nil {
		return "", err
	}
	file, err := os.Open(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("open %s file: %w", description, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect %s file: %w", description, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s file must be a regular file", description)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s file must not be accessible by group or other users (mode %04o)", description, info.Mode().Perm())
	}
	contents, err := io.ReadAll(io.LimitReader(file, maxSecretBytes+1))
	if err != nil {
		return "", fmt.Errorf("read %s file: %w", description, err)
	}
	if len(contents) > maxSecretBytes {
		return "", fmt.Errorf("%s file exceeds %d bytes", description, maxSecretBytes)
	}
	return validateSecret(string(contents), description+" file")
}

func validateSecret(value, source string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is empty", source)
	}
	if strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return "", fmt.Errorf("%s contains whitespace", source)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.New(source + " contains a NUL byte")
	}
	return value, nil
}
