package framework

import (
	"errors"
	"fmt"
	"regexp"
)

var contractName = regexp.MustCompile(`^[a-z][a-z0-9._/-]{0,127}$`)

// ErrContractMismatch means a port cannot satisfy a core requirement. It is a
// construction-time error, never a signal to silently downgrade at runtime.
var ErrContractMismatch = errors.New("framework contract mismatch")

type ContractID string
type Feature string

// Contract is a port's immutable protocol description. Atomicity and other
// base correctness requirements belong to the major contract itself; Features
// describe optional additions, never permissions.
type Contract struct {
	ID       ContractID
	Major    uint16
	Minor    uint16
	Features map[Feature]uint16
}

// Requirements declares what a consumer needs from one port.
type Requirements struct {
	ID           ContractID
	Major        uint16
	MinimumMinor uint16
	Features     map[Feature]uint16
}

// Negotiate verifies the contract once during construction and returns an
// independent frozen copy. Major versions must match exactly; providers may
// have a newer minor or feature revision.
func Negotiate(provided Contract, required Requirements) (Contract, error) {
	if err := validateContract(provided); err != nil {
		return Contract{}, err
	}
	if err := validateRequirements(required); err != nil {
		return Contract{}, err
	}
	if provided.ID != required.ID {
		return Contract{}, fmt.Errorf("%w: got id %q, want %q", ErrContractMismatch, provided.ID, required.ID)
	}
	if provided.Major != required.Major {
		return Contract{}, fmt.Errorf("%w: %s major %d, want %d", ErrContractMismatch, provided.ID, provided.Major, required.Major)
	}
	if provided.Minor < required.MinimumMinor {
		return Contract{}, fmt.Errorf("%w: %s minor %d is below required %d", ErrContractMismatch, provided.ID, provided.Minor, required.MinimumMinor)
	}
	for feature, version := range required.Features {
		actual, ok := provided.Features[feature]
		if !ok || actual < version {
			return Contract{}, fmt.Errorf("%w: %s feature %q version %d, want at least %d", ErrContractMismatch, provided.ID, feature, actual, version)
		}
	}
	return cloneContract(provided), nil
}

func validateContract(contract Contract) error {
	if !contractName.MatchString(string(contract.ID)) || contract.Major == 0 {
		return fmt.Errorf("%w: invalid provided contract identity/version", ErrContractMismatch)
	}
	for feature, version := range contract.Features {
		if !contractName.MatchString(string(feature)) || version == 0 {
			return fmt.Errorf("%w: invalid provided feature %q", ErrContractMismatch, feature)
		}
	}
	return nil
}

func validateRequirements(required Requirements) error {
	if !contractName.MatchString(string(required.ID)) || required.Major == 0 {
		return fmt.Errorf("%w: invalid required contract identity/version", ErrContractMismatch)
	}
	for feature, version := range required.Features {
		if !contractName.MatchString(string(feature)) || version == 0 {
			return fmt.Errorf("%w: invalid required feature %q", ErrContractMismatch, feature)
		}
	}
	return nil
}

func cloneContract(contract Contract) Contract {
	cloned := contract
	cloned.Features = make(map[Feature]uint16, len(contract.Features))
	for feature, version := range contract.Features {
		cloned.Features[feature] = version
	}
	return cloned
}
