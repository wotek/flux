package protobuf

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
)

var _ codec.Serializer = (*Serializer)(nil)

var (
	// ErrNotProtoMessage is returned when an event does not implement proto.Message.
	ErrNotProtoMessage = errors.New("event does not implement proto.Message")
)

// TypeRegistry resolves event names into concrete [flux.Event] instances.
type TypeRegistry interface {
	Instantiate(name string) (flux.Event, error)
}

// Serializer implements [codec.Serializer] using Protocol Buffers encoding.
type Serializer struct {
	types TypeRegistry
}

// New creates a new Protocol Buffers [Serializer] backed by the provided [TypeRegistry].
func New(types TypeRegistry) *Serializer {
	return &Serializer{types: types}
}

// Marshal encodes a [flux.Envelope] into Protocol Buffers bytes.
// The inner [flux.Event] must implement [proto.Message].
func (s *Serializer) Marshal(env flux.Envelope) ([]byte, error) {
	if env.Event == nil {
		return nil, fmt.Errorf("encoding envelope: %w", codec.ErrNilEvent)
	}

	protoMsg, ok := env.Event.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%w: event type %T", ErrNotProtoMessage, env.Event)
	}

	payload, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, fmt.Errorf("marshaling event payload: %w", err)
	}

	dto := EnvelopeDTO{
		Id:             env.Identifier.String(),
		EventName:      env.Event.Name(),
		EventPayload:   payload,
		Revision:       env.Revision,
		GlobalPosition: env.Position,
		Actor:          env.Actor.Identifier.String(),
		StreamId:       env.Stream.Identifier.String(),
		CorrelationId:  env.CorrelationIdentifier.String(),
		CausationId:    env.CausationIdentifier.String(),
		Metadata:       env.Metadata,
	}

	if !env.CreatedAt.IsZero() {
		dto.CreatedAt = timestamppb.New(env.CreatedAt)
	}

	data, err := proto.Marshal(&dto)
	if err != nil {
		return nil, fmt.Errorf("marshaling envelope: %w", err)
	}

	return data, nil
}

// Unmarshal decodes Protocol Buffers bytes into a [flux.Envelope].
func (s *Serializer) Unmarshal(data []byte) (flux.Envelope, error) {
	var dto EnvelopeDTO
	if err := proto.Unmarshal(data, &dto); err != nil {
		return flux.Envelope{}, fmt.Errorf("unmarshaling envelope: %w", err)
	}

	if dto.GetEventName() == "" {
		return flux.Envelope{}, fmt.Errorf("parsing envelope: %w", codec.ErrEmptyEventName)
	}

	eventPtr, err := s.types.Instantiate(dto.GetEventName())
	if err != nil {
		return flux.Envelope{}, fmt.Errorf("instantiating event %q: %w", dto.GetEventName(), err)
	}

	protoMsg, ok := eventPtr.(proto.Message)
	if !ok {
		return flux.Envelope{}, fmt.Errorf("%w: event type %T", ErrNotProtoMessage, eventPtr)
	}

	if len(dto.GetEventPayload()) > 0 {
		if err := proto.Unmarshal(dto.GetEventPayload(), protoMsg); err != nil {
			return flux.Envelope{}, fmt.Errorf("unmarshaling event payload for %q: %w", dto.GetEventName(), err)
		}
	}

	var identifier flux.Identifier
	if dto.GetId() != "" {
		var parseErr error
		identifier, parseErr = flux.ParseIdentifier(dto.GetId())
		if parseErr != nil {
			return flux.Envelope{}, fmt.Errorf("parsing identifier: %w", parseErr)
		}
	}

	var streamIdentifier flux.Identifier
	if dto.GetStreamId() != "" {
		var parseErr error
		streamIdentifier, parseErr = flux.ParseIdentifier(dto.GetStreamId())
		if parseErr != nil {
			return flux.Envelope{}, fmt.Errorf("parsing stream identifier: %w", parseErr)
		}
	}

	var actorIdentifier flux.Identifier
	if dto.GetActor() != "" {
		var parseErr error
		actorIdentifier, parseErr = flux.ParseIdentifier(dto.GetActor())
		if parseErr != nil {
			return flux.Envelope{}, fmt.Errorf("parsing actor identifier: %w", parseErr)
		}
	}

	var correlationIdentifier flux.Identifier
	if dto.GetCorrelationId() != "" {
		var parseErr error
		correlationIdentifier, parseErr = flux.ParseIdentifier(dto.GetCorrelationId())
		if parseErr != nil {
			return flux.Envelope{}, fmt.Errorf("parsing correlation identifier: %w", parseErr)
		}
	}

	var causationIdentifier flux.Identifier
	if dto.GetCausationId() != "" {
		var parseErr error
		causationIdentifier, parseErr = flux.ParseIdentifier(dto.GetCausationId())
		if parseErr != nil {
			return flux.Envelope{}, fmt.Errorf("parsing causation identifier: %w", parseErr)
		}
	}

	var createdAt time.Time
	if dto.GetCreatedAt() != nil {
		createdAt = dto.GetCreatedAt().AsTime()
	}

	var metadata map[string]string
	if len(dto.GetMetadata()) > 0 {
		metadata = make(map[string]string, len(dto.GetMetadata()))
		maps.Copy(metadata, dto.GetMetadata())
	}

	return flux.Envelope{
		Identifier:            identifier,
		Stream:                flux.Stream{Identifier: streamIdentifier},
		Revision:              dto.GetRevision(),
		Position:              dto.GetGlobalPosition(),
		Event:                 eventPtr,
		Metadata:              metadata,
		CreatedAt:             createdAt,
		Actor:                 flux.Actor{Identifier: actorIdentifier},
		CorrelationIdentifier: correlationIdentifier,
		CausationIdentifier:   causationIdentifier,
	}, nil
}
