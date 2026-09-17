package flux

import (
	"testing"
)

func TestIdentifier_Components(t *testing.T) {
	tests := []struct {
		urn          string
		org          string
		env          string
		svc          string
		account      string
		resourceType string
		resourceID   string
		version      string
	}{
		{
			urn:          "urn:acme:prod:payments:tenant-1:order:12345@v2",
			org:          "acme",
			env:          "prod",
			svc:          "payments",
			account:      "tenant-1",
			resourceType: "order",
			resourceID:   "12345",
			version:      "v2",
		},
		{
			urn:          "urn:acme::payments:tenant-1:order:12345/subpath",
			org:          "acme",
			env:          "",
			svc:          "payments",
			account:      "tenant-1",
			resourceType: "order",
			resourceID:   "12345/subpath",
			version:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.urn, func(t *testing.T) {
			id, err := ParseIdentifier(tt.urn)
			if err != nil {
				t.Fatalf("unexpected error parsing identifier: %v", err)
			}

			if got := id.Organization(); got != tt.org {
				t.Errorf("Organization() = %v, want %v", got, tt.org)
			}
			if got := id.Environment(); got != tt.env {
				t.Errorf("Environment() = %v, want %v", got, tt.env)
			}
			if got := id.Service(); got != tt.svc {
				t.Errorf("Service() = %v, want %v", got, tt.svc)
			}
			if got := id.AccountID(); got != tt.account {
				t.Errorf("AccountID() = %v, want %v", got, tt.account)
			}
			if got := id.ResourceType(); got != tt.resourceType {
				t.Errorf("ResourceType() = %v, want %v", got, tt.resourceType)
			}
			if got := id.ResourceID(); got != tt.resourceID {
				t.Errorf("ResourceID() = %v, want %v", got, tt.resourceID)
			}
			if got := id.Version(); got != tt.version {
				t.Errorf("Version() = %v, want %v", got, tt.version)
			}

			if id.String() != tt.urn {
				t.Errorf("String() = %v, want %v", id.String(), tt.urn)
			}
		})
	}
}

func TestIdentifier_Is(t *testing.T) {
	id, _ := ParseIdentifier("urn:org:env:svc:acc:type:id@v1")

	if !id.Is(ComponentOrganization, "org") {
		t.Error("expected ComponentOrganization to match")
	}
	if !id.Is(ComponentResourceType, "type") {
		t.Error("expected ComponentResourceType to match")
	}
	if id.Is(ComponentResourceType, "wrong") {
		t.Error("expected ComponentResourceType to not match")
	}
}

func TestNewIdentifier(t *testing.T) {
	id := NewIdentifier("org", "env", "svc", "acc", "type", "id/path", "v1")
	expected := "urn:org:env:svc:acc:type:id/path@v1"
	if id.String() != expected {
		t.Errorf("NewIdentifier() = %v, want %v", id.String(), expected)
	}
}
