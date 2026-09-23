package event_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

type userCreated struct {
	UserID string
	Email  string
}

var _ flux.Event = userCreated{}
var _ flux.Event = (*userCreated)(nil)

func (e userCreated) Name() string {
	return "UserCreated"
}

type userRenamed struct {
	NewName string
}

func (e userRenamed) Name() string {
	return "UserRenamed"
}

type pointerReceiverEvent struct {
	Note string
}

func (*pointerReceiverEvent) Name() string {
	return "PointerReceiverEvent"
}

func TestTypes_Instantiate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		register  func(*event.Types)
		lookup    string
		wantType  string
		wantError error
	}{
		{
			name: "registered event returns pointer to event",
			register: func(types *event.Types) {
				event.RegisterType[userCreated](types)
			},
			lookup:    "UserCreated",
			wantType:  "*event_test.userCreated",
			wantError: nil,
		},
		{
			name: "multiple registered events",
			register: func(types *event.Types) {
				event.RegisterType[userCreated](types)
				event.RegisterType[userRenamed](types)
			},
			lookup:    "UserRenamed",
			wantType:  "*event_test.userRenamed",
			wantError: nil,
		},
		{
			name: "custom factory registered via Register method",
			register: func(types *event.Types) {
				types.Register("UserCreated", func() flux.Event {
					return &userCreated{UserID: "custom"}
				})
			},
			lookup:    "UserCreated",
			wantType:  "*event_test.userCreated",
			wantError: nil,
		},
		{
			name: "pointer receiver registered via RegisterPointerType",
			register: func(types *event.Types) {
				event.RegisterPointerType[pointerReceiverEvent](types)
			},
			lookup:    "PointerReceiverEvent",
			wantType:  "*event_test.pointerReceiverEvent",
			wantError: nil,
		},
		{
			name:      "unregistered event returns ErrTypeNotRegistered",
			register:  func(_ *event.Types) {},
			lookup:    "OrderCancelled",
			wantError: event.ErrTypeNotRegistered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registry := event.NewTypes()
			tt.register(registry)

			ev, err := registry.Instantiate(tt.lookup)
			if tt.wantError != nil {
				if !errors.Is(err, tt.wantError) {
					t.Fatalf("Instantiate(%q) error = %v, want %v", tt.lookup, err, tt.wantError)
				}
				if ev != nil {
					t.Fatalf("Instantiate(%q) event = %v, want nil", tt.lookup, ev)
				}
				return
			}

			if err != nil {
				t.Fatalf("Instantiate(%q) unexpected error: %v", tt.lookup, err)
			}
			if ev == nil {
				t.Fatalf("Instantiate(%q) returned nil event", tt.lookup)
			}

			if gotName := ev.Name(); gotName != tt.lookup {
				t.Errorf("ev.Name() = %q, want %q", gotName, tt.lookup)
			}

			gotType := fmt.Sprintf("%T", ev)
			if gotType != tt.wantType {
				t.Errorf("Instantiate(%q) returned type %q, want %q", tt.lookup, gotType, tt.wantType)
			}
		})
	}
}

func TestTypes_Concurrent(t *testing.T) {
	t.Parallel()

	registry := event.NewTypes()
	event.RegisterType[userCreated](registry)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				event.RegisterType[userRenamed](registry)
			}
			ev, err := registry.Instantiate("UserCreated")
			if err != nil {
				t.Errorf("concurrent Instantiate failed: %v", err)
			}
			if _, ok := ev.(*userCreated); !ok {
				t.Errorf("unexpected type from concurrent Instantiate: %T", ev)
			}
		}(i)
	}

	wg.Wait()
}

func TestTypes_RegisterType_PanicsOnPointer(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected RegisterType to panic when passed a pointer")
		}
		if err, ok := r.(error); ok {
			if !errors.Is(err, event.ErrPointerRegistration) {
				t.Errorf("expected panic to be ErrPointerRegistration, got: %v", err)
			}
		}
	}()

	registry := event.NewTypes()
	// Attempting to register a pointer type should trigger the gatekeeper panic
	event.RegisterType[*userCreated](registry)
}
