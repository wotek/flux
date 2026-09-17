package client

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/projections/counter"
)

// RunWorkflow executes the canonical Todo CQRS demonstration workflow:
// 1. Adds 10 tasks (Task 1..10)
// 2. Removes odd tasks (Task 1, 3, 5, 7, 9)
// 3. Marks Task 6 and Task 10 as completed
// 4. Polls the counter read model until it converges to Active=3, Archived=2, Removed=5.
func RunWorkflow(ctx context.Context, c Client, listIdentifier flux.Identifier) (counter.Counter, error) {
	slog.InfoContext(ctx, "client workflow: adding 10 tasks...", "list", listIdentifier.String())
	for i := 1; i <= 10; i++ {
		taskName := fmt.Sprintf("Task %d", i)
		if err := c.AddTask(ctx, listIdentifier, taskName); err != nil {
			return counter.Counter{}, fmt.Errorf("adding %s: %w", taskName, err)
		}
	}

	slog.InfoContext(ctx, "client workflow: removing odd tasks (1, 3, 5, 7, 9)...")
	for i := 1; i <= 10; i += 2 {
		taskName := fmt.Sprintf("Task %d", i)
		if err := c.RemoveTask(ctx, listIdentifier, taskName); err != nil {
			return counter.Counter{}, fmt.Errorf("removing %s: %w", taskName, err)
		}
	}

	slog.InfoContext(ctx, "client workflow: marking Task 6 and Task 10 as completed...")
	if err := c.DoneTasks(ctx, listIdentifier, "Task 6", "Task 10"); err != nil {
		return counter.Counter{}, fmt.Errorf("marking tasks done: %w", err)
	}

	slog.InfoContext(ctx, "client workflow: awaiting read model projection convergence...")
	var snapshot counter.Counter
	var err error
	deadline := time.Now().Add(4 * time.Second)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return counter.Counter{}, ctx.Err()
		default:
		}

		snapshot, err = c.GetCounter(ctx)
		if err == nil && snapshot.Active == 3 && snapshot.Archived == 2 && snapshot.Removed == 5 {
			slog.InfoContext(ctx, "client workflow: read model converged",
				"active", snapshot.Active,
				"archived", snapshot.Archived,
				"removed", snapshot.Removed,
			)
			return snapshot, nil
		}
		time.Sleep(25 * time.Millisecond)
	}

	if err != nil {
		return counter.Counter{}, fmt.Errorf("querying counter read model: %w", err)
	}

	return snapshot, fmt.Errorf("timed out waiting for projection convergence, last state: %+v", snapshot)
}
