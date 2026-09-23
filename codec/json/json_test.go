package json_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/event"
)

type productCreated struct {
	ProductName string `json:"product_name"`
	Price       int    `json:"price"`
}

func (e productCreated) Name() string {
	return "ProductCreated"
}

func TestSerializer_RoundTrip(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterType[productCreated](registry)
	serializer := jsoncodec.New(registry)

	// Round(0) removes monotonic clock for exact reflect.DeepEqual match after unmarshal.
	fixedTime := time.Date(2026, 9, 23, 14, 30, 0, 0, time.UTC)

	original := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-101"),
		Stream: flux.Stream{
			Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
		},
		Revision: 3,
		Position: 42,
		Event: &productCreated{
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

	if !reflect.DeepEqual(original, unmarshaled) {
		t.Errorf("roundtrip mismatch:\ngot:  %+v\nwant: %+v", unmarshaled, original)
	}
}

func TestSerializer_Errors(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterType[productCreated](registry)
	serializer := jsoncodec.New(registry)

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
			name: "unmarshal malformed json returns error",
			action: func() error {
				_, err := serializer.Unmarshal([]byte("invalid json {{"))
				return err
			},
			targetErr: nil, // checked via err != nil
		},
		{
			name: "unmarshal missing event name returns ErrEmptyEventName",
			action: func() error {
				rawJSON := `{"id":"urn:acme:prod:catalog:tenant-1:event:evt-1"}`
				_, err := serializer.Unmarshal([]byte(rawJSON))
				return err
			},
			targetErr: codec.ErrEmptyEventName,
		},
		{
			name: "unmarshal unregistered event name returns ErrTypeNotRegistered",
			action: func() error {
				rawJSON := `{"id":"urn:acme:prod:catalog:tenant-1:event:evt-1","event_name":"UnknownEvent"}`
				_, err := serializer.Unmarshal([]byte(rawJSON))
				return err
			},
			targetErr: event.ErrTypeNotRegistered,
		},
		{
			name: "unmarshal malformed event payload returns error",
			action: func() error {
				rawJSON := `{"id":"urn:acme:prod:catalog:tenant-1:event:evt-1","event_name":"ProductCreated","event_payload":"not-json-object"}`
				_, err := serializer.Unmarshal([]byte(rawJSON))
				return err
			},
			targetErr: nil, // checked via err != nil
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

func TestSerializer_StreamFallback(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterType[productCreated](registry)
	serializer := jsoncodec.New(registry)

	// JSON payload using legacy "stream" field instead of "stream_id"
	rawJSON := `{
		"id": "urn:acme:prod:catalog:tenant-1:event:evt-1",
		"event_name": "ProductCreated",
		"event_payload": {"product_name": "Item", "price": 10},
		"stream": "urn:acme:prod:catalog:tenant-1:product:prod-99"
	}`

	env, err := serializer.Unmarshal([]byte(rawJSON))
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	wantStream := "urn:acme:prod:catalog:tenant-1:product:prod-99"
	if got := env.Stream.Identifier.String(); got != wantStream {
		t.Errorf("Stream Identifier = %q, want %q", got, wantStream)
	}
}
