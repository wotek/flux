package todo

// Counter represents the aggregated read-model view of todo task metrics.
type Counter struct {
	Active   int
	Archived int
	Removed  int
}
