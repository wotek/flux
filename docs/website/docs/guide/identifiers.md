# Structured Identifiers (URNs)

To ensure global uniqueness and make distributed debugging easier, `flux` relies heavily on Uniform Resource Names (URNs) for identifying streams, commands, and actors.

## The Identifier Type

Instead of passing around opaque strings or UUIDs, `flux` uses a typed `Identifier`.

```go
id := flux.MustParseIdentifier("urn:catalog:product:uuid-1234")
```

This enforces a standard structure across your entire system. If an identifier does not match the URN specification, it will fail to parse, catching errors early.

## Usage in Streams

Every Aggregate in the system belongs to a `Stream`, which requires an Identifier.

```go
stream := flux.Stream{
    Identifier: flux.MustParseIdentifier("urn:sales:order:9876"),
}
```
