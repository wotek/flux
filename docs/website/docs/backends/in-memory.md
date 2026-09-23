# In-Memory Backend

`flux` provides a blazing-fast, thread-safe In-Memory EventStore out of the box.

```go
import eventstore "github.com/wotek/flux/event/store"

store := eventstore.New()
```

## Use Cases
- **Unit Testing:** Perfect for testing Aggregates and Command Handlers without mocking databases.
- **Prototyping:** Build your entire domain logic before deciding on a production database.

*Note: The In-Memory store does not persist data across application restarts.*
