# Repositories & Snapshots

The `AggregateRepository` is the bridge between your in-memory Aggregates and your durable `EventStore`. 

## Loading an Aggregate

When you request an Aggregate from the repository, the framework:
1. Fetches the event stream for the given `Identifier`.
2. Instantiates an empty instance of your Aggregate by calling its `New()` method.
3. Loops through every historical event and passes it to your `apply()` method.

```go
stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:user:123")}
user, err := repo.Load(ctx, stream)
```

## Saving & Optimistic Concurrency

When `repo.Save()` is called, the repository extracts the uncommitted events from the aggregate's `Changeset`. It attempts to append them to the `EventStore` using the aggregate's current sequence version.

If another process modified the stream in the meantime, the `EventStore` will reject the append with `flux.ErrConcurrency`. This is the bedrock of consistency in event sourcing.

## Snapshots

For long-lived aggregates (like a Bank Account with 10,000 transactions), replaying history becomes a bottleneck. `flux` supports Snapshot stores. 

When configured, the repository will:
1. Load the latest Snapshot.
2. Fetch only the events that occurred *after* the snapshot's revision.
3. Replay the remaining events.
