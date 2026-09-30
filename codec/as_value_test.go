package codec_test

import (
	"reflect"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
)

type userCreated struct {
	UserID string
	Email  string
}

var _ flux.Event = userCreated{}

func (e userCreated) Name() string {
	return "UserCreated"
}

type pointerReceiverEvent struct {
	Note string
}

var _ flux.Event = (*pointerReceiverEvent)(nil)

func (e *pointerReceiverEvent) Name() string {
	return "PointerReceiverEvent"
}

func TestAsValue(t *testing.T) {
	t.Parallel()

	t.Run("nil event returns nil", func(t *testing.T) {
		t.Parallel()
		res := codec.AsValue(nil)
		if res != nil {
			t.Errorf("got %v, want nil", res)
		}
	})

	t.Run("nil typed pointer returns unchanged without dereferencing", func(t *testing.T) {
		t.Parallel()
		var ptr *userCreated
		res := codec.AsValue(ptr)
		if res == nil {
			t.Errorf("expected non-nil interface holding typed nil pointer, got nil")
		}
		if res != ptr {
			t.Errorf("got %v, want %v", res, ptr)
		}
	})

	t.Run("value event returns same value", func(t *testing.T) {
		t.Parallel()
		val := userCreated{UserID: "u1", Email: "u1@example.com"}
		res := codec.AsValue(val)
		if res != val {
			t.Errorf("got %v, want %v", res, val)
		}
		if reflect.TypeOf(res).Kind() == reflect.Pointer {
			t.Errorf("expected value kind, got pointer: %T", res)
		}
	})

	t.Run("pointer to value-receiver event returns value", func(t *testing.T) {
		t.Parallel()
		val := userCreated{UserID: "u2", Email: "u2@example.com"}
		res := codec.AsValue(&val)
		if res != val {
			t.Errorf("got %v, want %v", res, val)
		}
		if _, ok := res.(userCreated); !ok {
			t.Errorf("expected dynamic type userCreated, got %T", res)
		}
	})

	t.Run("pointer-receiver-only event returns pointer unchanged", func(t *testing.T) {
		t.Parallel()
		ptr := &pointerReceiverEvent{Note: "hello"}
		res := codec.AsValue(ptr)
		if res != ptr {
			t.Errorf("got %v, want %v", res, ptr)
		}
		if _, ok := res.(*pointerReceiverEvent); !ok {
			t.Errorf("expected dynamic type *pointerReceiverEvent, got %T", res)
		}
	})
}
