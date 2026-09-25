package flux

import (
	"encoding"
	"fmt"
	"strings"
)

// Component represents a part of the Identifier.
type Component int

const (
	ComponentOrganization Component = iota
	ComponentEnvironment
	ComponentService
	ComponentAccountID
	ComponentResourceType
	ComponentResourceID
	ComponentVersion
)

// Identifier represents a globally unique resource identifier.
// Format: urn:<organization>:<environment>:<service>:<account_id>:<resource_type>:<resource_id>[@<version>]
// Note: ResourceID may natively contain path separators ('/').
//
// Design note: Modeled after AWS Amazon Resource Names (ARNs, e.g. "arn:aws:s3:::bucket"),
// hierarchical components such as organization, environment, service, or account_id are
// intentionally optional. Empty components (represented by consecutive colons, e.g. "urn:::stream::event:id")
// are valid and supported when those scoping dimensions do not apply to the identified resource.
type Identifier struct {
	urn string
}

var (
	_ encoding.TextMarshaler   = Identifier{}
	_ encoding.TextUnmarshaler = (*Identifier)(nil)
)

// NewIdentifier constructs an Identifier from its constituent parts.
// Like AWS ARNs, constituent parts may be empty strings if that hierarchical scope is omitted.
func NewIdentifier(org, env, svc, account, resType, resID, version string) Identifier {
	base := fmt.Sprintf("urn:%s:%s:%s:%s:%s:%s", org, env, svc, account, resType, resID)
	if version != "" {
		base = fmt.Sprintf("%s@%s", base, version)
	}
	return Identifier{urn: base}
}

// ParseIdentifier parses a formatted URN string into an Identifier struct.
// It verifies the URN prefix and structural colon delimiter count (at least 6 colons for 7 segments).
// In accordance with AWS ARN conventions, individual components between colons may be empty strings.
func ParseIdentifier(s string) (Identifier, error) {
	if !strings.HasPrefix(s, "urn:") {
		return Identifier{}, fmt.Errorf("invalid identifier: must start with 'urn:'")
	}

	// Validate it has at least the minimum required colons (6 colons means 7 parts including 'urn')
	// urn:org:env:svc:acc:type:id
	parts := strings.SplitN(s, ":", 7)
	if len(parts) < 7 {
		return Identifier{}, fmt.Errorf("invalid identifier: missing components")
	}

	return Identifier{urn: s}, nil
}

// MustParseIdentifier constructs an Identifier directly from a formatted URN string.
// It is a convenience helper that delegates to ParseIdentifier and panics on error.
// It is intended exclusively for inline test declarations, static constants, and application bootstrap.
// For dynamic runtime input, use ParseIdentifier and handle errors appropriately.
func MustParseIdentifier(s string) Identifier {
	id, err := ParseIdentifier(s)
	if err != nil {
		panic(fmt.Errorf("MustParseIdentifier: %w", err))
	}
	return id
}

// IsEmpty returns true if the Identifier is the zero value (uninitialized or empty).
func (i Identifier) IsEmpty() bool {
	return i.urn == ""
}

// String returns the canonical string representation of the Identifier.
func (i Identifier) String() string {
	return i.urn
}

// MarshalText implements encoding.TextMarshaler.
func (i Identifier) MarshalText() ([]byte, error) {
	// The existing String() method perfectly outputs the formatted URN.
	return []byte(i.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (i *Identifier) UnmarshalText(text []byte) error {
	s := string(text)

	// Handle empty/null values gracefully
	if s == "" {
		*i = Identifier{}
		return nil
	}

	// Leverage the existing, robust validation logic
	parsed, err := ParseIdentifier(s)
	if err != nil {
		return err
	}

	*i = parsed
	return nil
}

// extractComponent extracts the value of a specific component based on colons.
func (i Identifier) extractComponent(index int) string {
	if i.urn == "" {
		return ""
	}

	// Skip 'urn:' prefix
	s := i.urn[4:]

	// Fast path for splitting
	currentPart := 0
	start := 0

	for j := 0; j < len(s); j++ {
		if s[j] == ':' {
			if currentPart == index {
				return s[start:j]
			}
			currentPart++
			start = j + 1
			// If we reached the ResourceID (index 5), everything else (up to '@') is the ID
			if currentPart == 5 {
				break
			}
		}
	}

	// Handle the last component (ResourceID and potentially Version)
	if currentPart == 5 && index == 5 {
		rem := s[start:]
		if atIdx := strings.LastIndex(rem, "@"); atIdx != -1 {
			return rem[:atIdx]
		}
		return rem
	}

	return ""
}

// Organization returns the top-level boundary.
func (i Identifier) Organization() string {
	return i.extractComponent(0)
}

// Environment returns the optional environment.
func (i Identifier) Environment() string {
	return i.extractComponent(1)
}

// Service returns the service namespace.
func (i Identifier) Service() string {
	return i.extractComponent(2)
}

// AccountID returns the customer, workspace, or org ID.
func (i Identifier) AccountID() string {
	return i.extractComponent(3)
}

// ResourceType returns the type of resource.
func (i Identifier) ResourceType() string {
	return i.extractComponent(4)
}

// ResourceID returns the identifier or path to the resource.
func (i Identifier) ResourceID() string {
	return i.extractComponent(5)
}

// Version returns the optional version.
func (i Identifier) Version() string {
	if i.urn == "" {
		return ""
	}
	if atIdx := strings.LastIndex(i.urn, "@"); atIdx != -1 {
		return i.urn[atIdx+1:]
	}
	return ""
}

// Is checks if a specific component of the identifier matches the given value.
func (i Identifier) Is(c Component, value string) bool {
	switch c {
	case ComponentOrganization:
		return i.Organization() == value
	case ComponentEnvironment:
		return i.Environment() == value
	case ComponentService:
		return i.Service() == value
	case ComponentAccountID:
		return i.AccountID() == value
	case ComponentResourceType:
		return i.ResourceType() == value
	case ComponentResourceID:
		return i.ResourceID() == value
	case ComponentVersion:
		return i.Version() == value
	default:
		return false
	}
}
