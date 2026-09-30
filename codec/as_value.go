package codec

import (
	"reflect"

	"github.com/wotek/flux"
)

// AsValue returns the domain-facing form of e after decode.
//
// Codecs Instantiate a *T to unmarshal into. For events registered with
// event.RegisterType[T], T implements flux.Event, so AsValue returns the value.
// For event.RegisterPointerType[T] (e.g. protobuf), only *T implements flux.Event,
// so AsValue leaves the pointer unchanged.
//
// Nil e is returned as nil. Non-pointer events are returned unchanged.
// Custom drivers that bypass Serializer should call AsValue after unmarshaling.
func AsValue(e flux.Event) flux.Event {
	if e == nil {
		return nil
	}

	val := reflect.ValueOf(e)
	if val.Kind() != reflect.Pointer || val.IsNil() {
		return e
	}

	elem := val.Elem()
	if ev, ok := elem.Interface().(flux.Event); ok {
		return ev
	}

	return e
}
