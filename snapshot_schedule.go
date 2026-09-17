package flux

// SnapshotSchedule determines if an aggregate should be snapshotted based on its current state.
type SnapshotSchedule[A any] interface {
	// Test returns true if a snapshot should be taken for the given aggregate.
	Test(aggregate A) bool
}

// Schedule is an alias for [SnapshotSchedule].
type Schedule[A any] = SnapshotSchedule[A]

// SnapshotScheduleFunc is a function adapter that implements [SnapshotSchedule].
type SnapshotScheduleFunc[A any] func(aggregate A) bool

// Test calls the underlying function to test if a snapshot should be taken.
func (f SnapshotScheduleFunc[A]) Test(aggregate A) bool {
	return f(aggregate)
}

type everySchedule[A Aggregate[A, E], E Event] struct {
	n uint64
}

// Test checks whether the aggregate revision is positive and a multiple of n.
func (s *everySchedule[A, E]) Test(aggregate A) bool {
	if s.n == 0 || aggregate.Revision() == 0 {
		return false
	}
	return aggregate.Revision()%s.n == 0
}

// Every returns a [SnapshotSchedule] that triggers every n events.
func Every[A Aggregate[A, E], E Event](n uint64) SnapshotSchedule[A] {
	return &everySchedule[A, E]{n: n}
}
