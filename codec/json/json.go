package json

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
)

var _ codec.Serializer = (*Serializer)(nil)

// TypeRegistry resolves event names into concrete [flux.Event] instances.
type TypeRegistry interface {
	Instantiate(name string) (flux.Event, error)
}

// envelopeDTO isolates JSON tags and serialization concerns from the core domain.
type envelopeDTO struct {
	Identifier            flux.Identifier   `json:"id"`
	EventName             string            `json:"event_name"`
	EventPayload          json.RawMessage   `json:"event_payload"`
	Revision              uint64            `json:"revision"`
	Position              uint64            `json:"global_position"`
	Actor                 flux.Identifier   `json:"actor,omitempty"`
	StreamIdentifier      flux.Identifier   `json:"stream_id"`
	StreamFallback        flux.Identifier   `json:"stream,omitempty"`
	CorrelationIdentifier flux.Identifier   `json:"correlation_id,omitempty"`
	CausationIdentifier   flux.Identifier   `json:"causation_id,omitempty"`
	CreatedAt             time.Time         `json:"created_at"`
	Metadata              map[string]string `json:"metadata,omitempty"`
}

// Serializer implements [codec.Serializer] using JSON encoding.
type Serializer struct {
	types TypeRegistry
}

// New creates a new JSON [Serializer] backed by the provided [TypeRegistry].
func New(types TypeRegistry) *Serializer {
	return &Serializer{types: types}
}

// Marshal encodes a [flux.Envelope] into JSON bytes.
func (s *Serializer) Marshal(env flux.Envelope) ([]byte, error) {
	if env.Event == nil {
		return nil, fmt.Errorf("encoding envelope: %w", codec.ErrNilEvent)
	}

	payload, err := json.Marshal(env.Event)
	if err != nil {
		return nil, fmt.Errorf("marshaling event payload: %w", err)
	}

	dto := envelopeDTO{
		Identifier:            env.Identifier,
		EventName:             env.Event.Name(),
		EventPayload:          payload,
		Revision:              env.Revision,
		Position:              env.Position,
		Actor:                 env.Actor.Identifier,
		StreamIdentifier:      env.Stream.Identifier,
		CorrelationIdentifier: env.CorrelationIdentifier,
		CausationIdentifier:   env.CausationIdentifier,
		CreatedAt:             env.CreatedAt,
		Metadata:              env.Metadata,
	}

	data, err := json.Marshal(dto)
	if err != nil {
		return nil, fmt.Errorf("marshaling envelope: %w", err)
	}

	return data, nil
}

// Unmarshal decodes JSON bytes into a [flux.Envelope].
func (s *Serializer) Unmarshal(data []byte) (flux.Envelope, error) {
	var dto envelopeDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return flux.Envelope{}, fmt.Errorf("unmarshaling envelope: %w", err)
	}

	if dto.EventName == "" {
		return flux.Envelope{}, fmt.Errorf("parsing envelope: %w", codec.ErrEmptyEventName)
	}

	eventPtr, err := s.types.Instantiate(dto.EventName)
	if err != nil {
		return flux.Envelope{}, fmt.Errorf("instantiating event %q: %w", dto.EventName, err)
	}

	if len(dto.EventPayload) > 0 {
		if err := json.Unmarshal(dto.EventPayload, eventPtr); err != nil {
			return flux.Envelope{}, fmt.Errorf("unmarshaling event payload for %q: %w", dto.EventName, err)
		}
	}

	streamID := dto.StreamIdentifier
	if streamID.IsEmpty() && !dto.StreamFallback.IsEmpty() {
		streamID = dto.StreamFallback
	}

	return flux.Envelope{
		Identifier:            dto.Identifier,
		Stream:                flux.Stream{Identifier: streamID},
		Revision:              dto.Revision,
		Position:              dto.Position,
		Event:                 eventPtr,
		Metadata:              dto.Metadata,
		CreatedAt:             dto.CreatedAt,
		Actor:                 flux.Actor{Identifier: dto.Actor},
		CorrelationIdentifier: dto.CorrelationIdentifier,
		CausationIdentifier:   dto.CausationIdentifier,
	}, nil
}
