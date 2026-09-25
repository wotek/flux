# Installation

`flux` is designed for modern Go and relies heavily on Go 1.27+ generic features to provide a reflection-free, type-safe API.

## Requirements
- **Go 1.27 or higher**

## Installing

Install the package using the standard `go get` command:

```bash
go get github.com/wotek/flux
```

## Importing Core Packages

`flux` is modular by design. You only import the boundaries you need:

```go
import (
    "github.com/wotek/flux"                  // Core types (Events, Aggregates, Streams)
    "github.com/wotek/flux/command"          // CQRS Command Bus
    "github.com/wotek/flux/event"            // Pub/Sub Event Bus
    eventstore "github.com/wotek/flux/event/store" // Built-in Event Stores
)
```
