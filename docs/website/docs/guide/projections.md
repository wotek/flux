# Projections

Projections (Read Models) are optimized views of your data designed specifically for querying. Because events are the source of truth, you can project them into any database (SQL, Redis, MongoDB) in whatever shape the frontend requires.

## Building a Read Model

A projector listens to the event stream (via the `EventBus`) and translates domain events into database mutations.

```go
func ProjectUserView(ctx context.Context, db *sql.DB, event flux.Event) error {
    switch e := event.(type) {
    case UserRegistered:
        _, err := db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", e.ID, e.Email)
        return err
    case EmailChanged:
        _, err := db.Exec("UPDATE users SET email = ? WHERE id = ?", e.NewEmail, e.ID)
        return err
    }
    return nil
}
```

By decoupling the read model from the write model, your queries can be blazing fast `SELECT * FROM users` statements without any complex joins or domain logic.
