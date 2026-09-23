package xml

import (
	"encoding/xml"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
)

var _ codec.Serializer = (*Serializer)(nil)

// TypeRegistry resolves event names into concrete [flux.Event] instances.
type TypeRegistry interface {
	Instantiate(name string) (flux.Event, error)
}

// rawXML encapsulates arbitrary inner XML content without altering tags.
type rawXML struct {
	Inner []byte `xml:",innerxml"`
}

// metadataEntry represents a single key-value attribute pair in XML.
type metadataEntry struct {
	Key   string `xml:"key,attr"`
	Value string `xml:",chardata"`
}

// metadataDTO wraps envelope metadata into XML-compatible entry elements.
type metadataDTO struct {
	Entries []metadataEntry `xml:"entry"`
}

// envelopeDTO isolates XML tags and serialization concerns from the core domain.
type envelopeDTO struct {
	XMLName               xml.Name          `xml:"envelope"`
	Identifier            flux.Identifier   `xml:"id"`
	EventName             string            `xml:"event_name"`
	EventPayload          rawXML            `xml:"event_payload"`
	Revision              uint64            `xml:"revision"`
	Position              uint64            `xml:"global_position"`
	Actor                 flux.Identifier   `xml:"actor,omitempty"`
	StreamIdentifier      flux.Identifier   `xml:"stream_id"`
	StreamFallback        flux.Identifier   `xml:"stream,omitempty"`
	CorrelationIdentifier flux.Identifier   `xml:"correlation_id,omitempty"`
	CausationIdentifier   flux.Identifier   `xml:"causation_id,omitempty"`
	CreatedAt             time.Time         `xml:"created_at"`
	Metadata              *metadataDTO      `xml:"metadata,omitempty"`
}

// Serializer implements [codec.Serializer] using XML encoding.
type Serializer struct {
	types TypeRegistry
}

// New creates a new XML [Serializer] backed by the provided [TypeRegistry].
func New(types TypeRegistry) *Serializer {
	return &Serializer{types: types}
}

// Marshal encodes a [flux.Envelope] into XML bytes.
func (s *Serializer) Marshal(env flux.Envelope) ([]byte, error) {
	if env.Event == nil {
		return nil, fmt.Errorf("encoding envelope: %w", codec.ErrNilEvent)
	}

	payload, err := xml.Marshal(env.Event)
	if err != nil {
		return nil, fmt.Errorf("marshaling event payload: %w", err)
	}

	dto := envelopeDTO{
		Identifier:            env.Identifier,
		EventName:             env.Event.Name(),
		EventPayload:          rawXML{Inner: payload},
		Revision:              env.Revision,
		Position:              env.Position,
		Actor:                 env.Actor.Identifier,
		StreamIdentifier:      env.Stream.Identifier,
		CorrelationIdentifier: env.CorrelationIdentifier,
		CausationIdentifier:   env.CausationIdentifier,
		CreatedAt:             env.CreatedAt,
	}

	if len(env.Metadata) > 0 {
		entries := make([]metadataEntry, 0, len(env.Metadata))
		for _, k := range slices.Sorted(maps.Keys(env.Metadata)) {
			entries = append(entries, metadataEntry{
				Key:   k,
				Value: env.Metadata[k],
			})
		}
		dto.Metadata = &metadataDTO{Entries: entries}
	}

	data, err := xml.Marshal(dto)
	if err != nil {
		return nil, fmt.Errorf("marshaling envelope: %w", err)
	}

	return data, nil
}

// Unmarshal decodes XML bytes into a [flux.Envelope].
func (s *Serializer) Unmarshal(data []byte) (flux.Envelope, error) {
	var dto envelopeDTO
	if err := xml.Unmarshal(data, &dto); err != nil {
		return flux.Envelope{}, fmt.Errorf("unmarshaling envelope: %w", err)
	}

	if dto.EventName == "" {
		return flux.Envelope{}, fmt.Errorf("parsing envelope: %w", codec.ErrEmptyEventName)
	}

	eventPtr, err := s.types.Instantiate(dto.EventName)
	if err != nil {
		return flux.Envelope{}, fmt.Errorf("instantiating event %q: %w", dto.EventName, err)
	}

	if len(dto.EventPayload.Inner) > 0 {
		if err := xml.Unmarshal(dto.EventPayload.Inner, eventPtr); err != nil {
			return flux.Envelope{}, fmt.Errorf("unmarshaling event payload for %q: %w", dto.EventName, err)
		}
	}

	streamID := dto.StreamIdentifier
	if streamID.IsEmpty() && !dto.StreamFallback.IsEmpty() {
		streamID = dto.StreamFallback
	}

	var metadata map[string]string
	if dto.Metadata != nil && len(dto.Metadata.Entries) > 0 {
		metadata = make(map[string]string, len(dto.Metadata.Entries))
		for _, entry := range dto.Metadata.Entries {
			metadata[entry.Key] = entry.Value
		}
	}

	return flux.Envelope{
		Identifier:            dto.Identifier,
		Stream:                flux.Stream{Identifier: streamID},
		Revision:              dto.Revision,
		Position:              dto.Position,
		Event:                 eventPtr,
		Metadata:              metadata,
		CreatedAt:             dto.CreatedAt,
		Actor:                 flux.Actor{Identifier: dto.Actor},
		CorrelationIdentifier: dto.CorrelationIdentifier,
		CausationIdentifier:   dto.CausationIdentifier,
	}, nil
}
