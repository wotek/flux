package flux

import (
	"encoding/json"
	"testing"
)

func TestIdentifier_Components(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	id := NewIdentifier("org", "env", "svc", "acc", "type", "id/path", "v1")
	expected := "urn:org:env:svc:acc:type:id/path@v1"
	if id.String() != expected {
		t.Errorf("NewIdentifier() = %v, want %v", id.String(), expected)
	}
}

func TestIdentifier_TextSerialization(t *testing.T) {
	t.Parallel()
	id := MustParseIdentifier("urn:acme:prod:payments:tenant-1:order:12345@v2")

	// MarshalText
	text, err := id.MarshalText()
	if err != nil {
		t.Fatalf("unexpected error during MarshalText: %v", err)
	}
	if string(text) != id.String() {
		t.Errorf("MarshalText() = %q, want %q", string(text), id.String())
	}

	// UnmarshalText valid
	var unmarshaled Identifier
	if err := unmarshaled.UnmarshalText(text); err != nil {
		t.Fatalf("unexpected error during UnmarshalText: %v", err)
	}
	if unmarshaled != id {
		t.Errorf("UnmarshalText() = %v, want %v", unmarshaled, id)
	}

	// UnmarshalText empty string
	var empty Identifier
	if err := empty.UnmarshalText([]byte("")); err != nil {
		t.Fatalf("unexpected error unmarshaling empty text: %v", err)
	}
	if !empty.IsEmpty() {
		t.Errorf("expected empty identifier to be empty, got: %v", empty)
	}

	// UnmarshalText invalid format
	var invalid Identifier
	if err := invalid.UnmarshalText([]byte("not-a-urn")); err == nil {
		t.Fatal("expected error unmarshaling invalid text, got nil")
	}
}

func TestIdentifier_JSONSerialization(t *testing.T) {
	t.Parallel()
	type wrapper struct {
		ID   Identifier `json:"id"`
		Name string     `json:"name"`
	}

	orig := wrapper{
		ID:   MustParseIdentifier("urn:acme:prod:payments:tenant-1:order:12345@v2"),
		Name: "Test Order",
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	expectedJSON := `{"id":"urn:acme:prod:payments:tenant-1:order:12345@v2","name":"Test Order"}`
	if string(data) != expectedJSON {
		t.Errorf("json.Marshal() = %s, want %s", string(data), expectedJSON)
	}

	var decoded wrapper
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded != orig {
		t.Errorf("decoded = %+v, want %+v", decoded, orig)
	}

	// Deserializing empty string
	emptyJSON := `{"id":"","name":"Empty Order"}`
	var emptyDecoded wrapper
	if err := json.Unmarshal([]byte(emptyJSON), &emptyDecoded); err != nil {
		t.Fatalf("json.Unmarshal empty ID failed: %v", err)
	}
	if !emptyDecoded.ID.IsEmpty() {
		t.Errorf("expected empty ID, got %v", emptyDecoded.ID)
	}

	// Deserializing invalid identifier
	invalidJSON := `{"id":"invalid-urn","name":"Invalid"}`
	var invalidDecoded wrapper
	if err := json.Unmarshal([]byte(invalidJSON), &invalidDecoded); err == nil {
		t.Fatal("expected json.Unmarshal to fail for invalid URN, got nil")
	}
}
