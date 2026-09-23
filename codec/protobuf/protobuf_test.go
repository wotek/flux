package protobuf_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
	protocodec "github.com/wotek/flux/codec/protobuf"
	"github.com/wotek/flux/codec/protobuf/internal/testpb"
	"github.com/wotek/flux/event"
)

type nonProtoEvent struct {
	Title string
}

func (e nonProtoEvent) Name() string {
	return "NonProtoEvent"
}

func TestSerializer_RoundTrip(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterPointerType[testpb.TestProductCreated](registry)
	serializer := protocodec.New(registry)

	fixedTime := time.Date(2026, 9, 23, 14, 30, 0, 0, time.UTC)

	original := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-101"),
		Stream: flux.Stream{
			Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
		},
		Revision: 3,
		Position: 42,
		Event: &testpb.TestProductCreated{
			ProductName: "Mechanical Keyboard",
			Price:       150,
		},
		Metadata: map[string]string{
			"trace_id": "trace-xyz",
			"client":   "web-ui",
		},
		CreatedAt: fixedTime,
		Actor: flux.Actor{
			Identifier: flux.MustParseIdentifier("urn:acme:prod:iam:tenant-1:user:usr-404"),
		},
		CorrelationIdentifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:command:cmd-555"),
		CausationIdentifier:   flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-100"),
	}

	data, err := serializer.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	unmarshaled, err := serializer.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if unmarshaled.Identifier != original.Identifier {
		t.Errorf("Identifier = %v, want %v", unmarshaled.Identifier, original.Identifier)
	}
	if unmarshaled.Stream != original.Stream {
		t.Errorf("Stream = %v, want %v", unmarshaled.Stream, original.Stream)
	}
	if unmarshaled.Revision != original.Revision {
		t.Errorf("Revision = %v, want %v", unmarshaled.Revision, original.Revision)
	}
	if unmarshaled.Position != original.Position {
		t.Errorf("Position = %v, want %v", unmarshaled.Position, original.Position)
	}
	if unmarshaled.Actor != original.Actor {
		t.Errorf("Actor = %v, want %v", unmarshaled.Actor, original.Actor)
	}
	if unmarshaled.CorrelationIdentifier != original.CorrelationIdentifier {
		t.Errorf("CorrelationIdentifier = %v, want %v", unmarshaled.CorrelationIdentifier, original.CorrelationIdentifier)
	}
	if unmarshaled.CausationIdentifier != original.CausationIdentifier {
		t.Errorf("CausationIdentifier = %v, want %v", unmarshaled.CausationIdentifier, original.CausationIdentifier)
	}
	if !unmarshaled.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", unmarshaled.CreatedAt, original.CreatedAt)
	}
	if !reflect.DeepEqual(unmarshaled.Metadata, original.Metadata) {
		t.Errorf("Metadata = %v, want %v", unmarshaled.Metadata, original.Metadata)
	}

	origMsg, ok1 := original.Event.(proto.Message)
	unmarshMsg, ok2 := unmarshaled.Event.(proto.Message)
	if !ok1 || !ok2 || !proto.Equal(origMsg, unmarshMsg) {
		t.Errorf("Event proto mismatch:\ngot:  %+v\nwant: %+v", unmarshaled.Event, original.Event)
	}
}

func TestSerializer_Errors(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterPointerType[testpb.TestProductCreated](registry)
	event.RegisterType[nonProtoEvent](registry)
	serializer := protocodec.New(registry)

	tests := []struct {
		name      string
		action    func() error
		targetErr error
	}{
		{
			name: "marshal nil event returns ErrNilEvent",
			action: func() error {
				env := flux.Envelope{
					Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-1"),
					Event:      nil,
				}
				_, err := serializer.Marshal(env)
				return err
			},
			targetErr: codec.ErrNilEvent,
		},
		{
			name: "marshal non-proto message returns ErrNotProtoMessage",
			action: func() error {
				env := flux.Envelope{
					Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-1"),
					Event:      nonProtoEvent{Title: "hello"},
				}
				_, err := serializer.Marshal(env)
				return err
			},
			targetErr: protocodec.ErrNotProtoMessage,
		},
		{
			name: "unmarshal malformed proto returns error",
			action: func() error {
				_, err := serializer.Unmarshal([]byte{0xff, 0xff, 0xff})
				return err
			},
			targetErr: nil,
		},
		{
			name: "unmarshal missing event name returns ErrEmptyEventName",
			action: func() error {
				dto := protocodec.EnvelopeDTO{
					Id: "urn:acme:prod:catalog:tenant-1:event:evt-1",
				}
				bytes, err := proto.Marshal(&dto)
				if err != nil {
					return err
				}
				_, err = serializer.Unmarshal(bytes)
				return err
			},
			targetErr: codec.ErrEmptyEventName,
		},
		{
			name: "unmarshal unregistered event name returns ErrTypeNotRegistered",
			action: func() error {
				dto := protocodec.EnvelopeDTO{
					Id:        "urn:acme:prod:catalog:tenant-1:event:evt-1",
					EventName: "UnregisteredEvent",
				}
				bytes, err := proto.Marshal(&dto)
				if err != nil {
					return err
				}
				_, err = serializer.Unmarshal(bytes)
				return err
			},
			targetErr: event.ErrTypeNotRegistered,
		},
		{
			name: "unmarshal non-proto registered event returns ErrNotProtoMessage",
			action: func() error {
				dto := protocodec.EnvelopeDTO{
					Id:        "urn:acme:prod:catalog:tenant-1:event:evt-1",
					EventName: "NonProtoEvent",
				}
				bytes, err := proto.Marshal(&dto)
				if err != nil {
					return err
				}
				_, err = serializer.Unmarshal(bytes)
				return err
			},
			targetErr: protocodec.ErrNotProtoMessage,
		},
		{
			name: "unmarshal malformed event payload returns error",
			action: func() error {
				dto := protocodec.EnvelopeDTO{
					Id:           "urn:acme:prod:catalog:tenant-1:event:evt-1",
					EventName:    "TestProductCreated",
					EventPayload: []byte{0xff, 0xff, 0xff},
				}
				bytes, err := proto.Marshal(&dto)
				if err != nil {
					return err
				}
				_, err = serializer.Unmarshal(bytes)
				return err
			},
			targetErr: nil,
		},
		{
			name: "unmarshal invalid identifier returns error",
			action: func() error {
				dto := protocodec.EnvelopeDTO{
					Id:        "invalid-urn",
					EventName: "TestProductCreated",
				}
				bytes, err := proto.Marshal(&dto)
				if err != nil {
					return err
				}
				_, err = serializer.Unmarshal(bytes)
				return err
			},
			targetErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.action()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if tt.targetErr != nil && !errors.Is(err, tt.targetErr) {
				t.Fatalf("expected error wrapping %v, got %v", tt.targetErr, err)
			}
		})
	}
}

func TestSerializer_EmptyOptionalFields(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterPointerType[testpb.TestProductCreated](registry)
	serializer := protocodec.New(registry)

	original := flux.Envelope{
		Event: &testpb.TestProductCreated{
			ProductName: "Basic Item",
			Price:       20,
		},
	}

	data, err := serializer.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	unmarshaled, err := serializer.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if !unmarshaled.Identifier.IsEmpty() {
		t.Errorf("expected empty Identifier, got %v", unmarshaled.Identifier)
	}
	if !unmarshaled.Stream.Identifier.IsEmpty() {
		t.Errorf("expected empty Stream, got %v", unmarshaled.Stream)
	}
	if !unmarshaled.Actor.Identifier.IsEmpty() {
		t.Errorf("expected empty Actor, got %v", unmarshaled.Actor)
	}
	if !unmarshaled.CorrelationIdentifier.IsEmpty() {
		t.Errorf("expected empty CorrelationIdentifier, got %v", unmarshaled.CorrelationIdentifier)
	}
	if !unmarshaled.CausationIdentifier.IsEmpty() {
		t.Errorf("expected empty CausationIdentifier, got %v", unmarshaled.CausationIdentifier)
	}
	if !unmarshaled.CreatedAt.IsZero() {
		t.Errorf("expected zero CreatedAt, got %v", unmarshaled.CreatedAt)
	}
	if unmarshaled.Metadata != nil {
		t.Errorf("expected nil Metadata, got %v", unmarshaled.Metadata)
	}

	origMsg := original.Event.(proto.Message)
	unmarshMsg, ok := unmarshaled.Event.(proto.Message)
	if !ok || !proto.Equal(origMsg, unmarshMsg) {
		t.Errorf("Event proto mismatch:\ngot:  %+v\nwant: %+v", unmarshaled.Event, original.Event)
	}
}
