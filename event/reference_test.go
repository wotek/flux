package event_test

import (
	jsonv1 "encoding/json"
	jsonv2 "encoding/json/v2"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

func TestEventReference_JSONv2RoundTrip(t *testing.T) {
	t.Parallel()

	streamID := flux.MustParseIdentifier("urn:test:prod:order:tenant-1:order:ord-123")
	eventID := flux.MustParseIdentifier("urn:test:prod:order:tenant-1:event:evt-456")

	ref := event.EventReference{
		Stream:  streamID,
		EventID: eventID,
	}

	data, err := jsonv2.Marshal(ref)
	if err != nil {
		t.Fatalf("jsonv2.Marshal failed: %v", err)
	}

	expectedJSON := `{"stream":"urn:test:prod:order:tenant-1:order:ord-123","event_id":"urn:test:prod:order:tenant-1:event:evt-456"}`
	if string(data) != expectedJSON {
		t.Errorf("got JSON %s, want %s", string(data), expectedJSON)
	}

	var decoded event.EventReference
	if err := jsonv2.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("jsonv2.Unmarshal failed: %v", err)
	}

	if decoded.Stream != streamID {
		t.Errorf("decoded.Stream = %s, want %s", decoded.Stream, streamID)
	}
	if decoded.EventID != eventID {
		t.Errorf("decoded.EventID = %s, want %s", decoded.EventID, eventID)
	}
}

func TestEventReference_JSONv1Compatibility(t *testing.T) {
	t.Parallel()

	streamID := flux.MustParseIdentifier("urn:test:prod:order:tenant-1:order:ord-123")
	eventID := flux.MustParseIdentifier("urn:test:prod:order:tenant-1:event:evt-456")

	ref := event.EventReference{
		Stream:  streamID,
		EventID: eventID,
	}

	// Marshal with v2, unmarshal with v1
	dataV2, err := jsonv2.Marshal(ref)
	if err != nil {
		t.Fatalf("jsonv2.Marshal failed: %v", err)
	}

	var decodedV1 event.EventReference
	if err := jsonv1.Unmarshal(dataV2, &decodedV1); err != nil {
		t.Fatalf("jsonv1.Unmarshal failed: %v", err)
	}
	if decodedV1 != ref {
		t.Errorf("decodedV1 = %+v, want %+v", decodedV1, ref)
	}

	// Marshal with v1, unmarshal with v2
	dataV1, err := jsonv1.Marshal(ref)
	if err != nil {
		t.Fatalf("jsonv1.Marshal failed: %v", err)
	}

	var decodedV2 event.EventReference
	if err := jsonv2.Unmarshal(dataV1, &decodedV2); err != nil {
		t.Fatalf("jsonv2.Unmarshal failed: %v", err)
	}
	if decodedV2 != ref {
		t.Errorf("decodedV2 = %+v, want %+v", decodedV2, ref)
	}
}

func TestEventReference_ZeroValue(t *testing.T) {
	t.Parallel()

	var zero event.EventReference
	if !zero.Stream.IsEmpty() {
		t.Errorf("expected zero Stream to be empty")
	}
	if !zero.EventID.IsEmpty() {
		t.Errorf("expected zero EventID to be empty")
	}

	data, err := jsonv2.Marshal(zero)
	if err != nil {
		t.Fatalf("jsonv2.Marshal failed for zero value: %v", err)
	}

	var decoded event.EventReference
	if err := jsonv2.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("jsonv2.Unmarshal failed for zero value: %v", err)
	}

	if !decoded.Stream.IsEmpty() {
		t.Errorf("expected decoded zero Stream to be empty")
	}
	if !decoded.EventID.IsEmpty() {
		t.Errorf("expected decoded zero EventID to be empty")
	}
}
