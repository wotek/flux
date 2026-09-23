package xml_test

import (
	"encoding/xml"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
	xmlcodec "github.com/wotek/flux/codec/xml"
	"github.com/wotek/flux/event"
)

type productCreated struct {
	XMLName     xml.Name `xml:"ProductCreated"`
	ProductName string   `xml:"product_name"`
	Price       int      `xml:"price"`
}

func (e productCreated) Name() string {
	return "ProductCreated"
}

func TestSerializer_RoundTrip(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterType[productCreated](registry)
	serializer := xmlcodec.New(registry)

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
			XMLName:     xml.Name{Local: "ProductCreated"},
			ProductName: "Mechanical Keyboard",
			Price:       150,
		},
		Metadata: map[string]string{
			"client":   "web-ui",
			"trace_id": "trace-xyz",
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
	serializer := xmlcodec.New(registry)

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
			name: "unmarshal malformed xml returns error",
			action: func() error {
				_, err := serializer.Unmarshal([]byte("<envelope><unclosed>"))
				return err
			},
			targetErr: nil, // checked via err != nil
		},
		{
			name: "unmarshal missing event name returns ErrEmptyEventName",
			action: func() error {
				rawXML := `<envelope><id>urn:acme:prod:catalog:tenant-1:event:evt-1</id></envelope>`
				_, err := serializer.Unmarshal([]byte(rawXML))
				return err
			},
			targetErr: codec.ErrEmptyEventName,
		},
		{
			name: "unmarshal unregistered event name returns ErrTypeNotRegistered",
			action: func() error {
				rawXML := `<envelope><id>urn:acme:prod:catalog:tenant-1:event:evt-1</id><event_name>UnknownEvent</event_name></envelope>`
				_, err := serializer.Unmarshal([]byte(rawXML))
				return err
			},
			targetErr: event.ErrTypeNotRegistered,
		},
		{
			name: "unmarshal malformed event payload returns error",
			action: func() error {
				rawXML := `<envelope><id>urn:acme:prod:catalog:tenant-1:event:evt-1</id><event_name>ProductCreated</event_name><event_payload><ProductCreated><price>invalid-int</price></ProductCreated></event_payload></envelope>`
				_, err := serializer.Unmarshal([]byte(rawXML))
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
	serializer := xmlcodec.New(registry)

	// XML payload using legacy "stream" tag instead of "stream_id"
	rawXML := `<envelope>
		<id>urn:acme:prod:catalog:tenant-1:event:evt-1</id>
		<event_name>ProductCreated</event_name>
		<event_payload><ProductCreated><product_name>Item</product_name><price>10</price></ProductCreated></event_payload>
		<stream>urn:acme:prod:catalog:tenant-1:product:prod-99</stream>
	</envelope>`

	env, err := serializer.Unmarshal([]byte(rawXML))
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	wantStream := "urn:acme:prod:catalog:tenant-1:product:prod-99"
	if got := env.Stream.Identifier.String(); got != wantStream {
		t.Errorf("Stream Identifier = %q, want %q", got, wantStream)
	}
}
