# Serialization & Codecs

A fundamental design philosophy of `flux` is that **the core domain must remain completely unaware of how it is serialized**. 

If you look at the `flux.Envelope` or your own Domain Events (e.g., `ProductCreated`), you will not find a single `json:`, `xml:`, or `bson:` struct tag. Keeping these infrastructure concerns out of your domain prevents vendor lock-in and allows your system to seamlessly adapt to any database driver or message broker.

So, how do backend drivers (like MySQL, Redis, or Kafka) actually serialize an envelope to bytes?

They use the built-in **Type Registry** and **Serialization Codecs**.

---

## The Challenge: Deserializing Interfaces

The `flux.Envelope` struct contains the inner domain event as a Go interface: `Event flux.Event`.

Because it is an interface, standard Go functions like `json.Unmarshal()` or `xml.Unmarshal()` will fail to decode it. The Go compiler has no way of knowing whether the raw JSON bytes represent a `ProductCreated` struct or an `OrderPlaced` struct.

To solve this, a framework must provide a **Type Registry** that converts a string name back into a concrete Go pointer.

---

## 1. The Generic Type Registry (`event.Types`)

`flux` solves the interface deserialization problem natively, **without using Go's slow `reflect` package**. 

Instead, it relies on Go Generics to provide a blazing-fast, type-safe registry in the `flux/event` package.

### Registering Events
During your application's startup, you register your events with the generic `Types` registry. This tells the framework how to instantiate empty pointers of your events.

```go
import (
    "github.com/wotek/flux/event"
    "e-commerce/internal/catalog/events"
)

// Create a new registry
registry := event.NewTypes()

// Register your events using pure Generics!
event.RegisterType[events.ProductCreated](registry)
event.RegisterType[events.PriceUpdated](registry)
```

### Instantiating Events (For Backend Drivers)
If you are writing a custom database driver, you simply ask the registry for an empty pointer using the event's name.

```go
// 1. Get an empty *events.ProductCreated pointer wrapped in the Event interface
eventPtr, err := registry.Instantiate("ProductCreated")

// 2. Unmarshal raw JSON bytes directly into the interface value!
// Go is smart enough to unpack the interface and populate the underlying pointer.
json.Unmarshal(rawBytes, eventPtr)

// 3. Assign it safely back to the Envelope
envelope.Event = eventPtr
```

---

## 2. Serialization Codecs

To save backend driver authors from reinventing the wheel, `flux` provides "Batteries-Included" codecs. These codecs handle the complex mapping of the `flux.Envelope` into private Data Transfer Objects (DTOs) safely.

### The Codec Interface
All codecs implement a dead-simple interface:

```go
package codec

type Serializer interface {
	Marshal(env flux.Envelope) ([]byte, error)
	Unmarshal(data []byte) (flux.Envelope, error)
}
```

### Supported Codecs

`flux` currently ships with two natively supported codecs: **JSON** and **XML**.

#### Using the JSON Codec
The JSON codec provides standard, human-readable serialization. It is perfect for logging, REST APIs, or document databases like MongoDB and PostgreSQL JSONB columns.

```go
import (
    "github.com/wotek/flux/codec/json"
    "github.com/wotek/flux/event"
)

// Initialize your registry
registry := event.NewTypes()
event.RegisterType[ProductCreated](registry)

// Initialize the codec
jsonCodec := json.New(registry)

// Marshal an envelope to bytes
bytes, _ := jsonCodec.Marshal(myEnvelope)

// Unmarshal bytes back into an envelope
rebuiltEnv, _ := jsonCodec.Unmarshal(bytes)
```

#### Using the XML Codec
The XML codec behaves identically but outputs strict XML. It securely maps map metadata into `<entry>` blocks and uses `<innerxml>` wrapping to serialize the domain event safely.

```go
import "github.com/wotek/flux/codec/xml"

xmlCodec := xml.New(registry)
bytes, _ := xmlCodec.Marshal(myEnvelope)
```

### Writing Your Own Codec (e.g., Protobuf)

If your enterprise requires extreme performance or cross-language compatibility, you can easily write your own `ProtobufSerializer` or `MessagePackSerializer`. 

Simply define a struct that implements `codec.Serializer`, pass the `event.Types` registry into its constructor, and provide it to your backend driver!
