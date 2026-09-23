package mysql

// Option configures a MySQL [SnapshotStore].
type Option func(*config)

type config struct {
	tableName string
}

func defaultConfig() config {
	return config{
		tableName: "snapshots",
	}
}

// WithTableName sets a custom table name for the snapshots table.
func WithTableName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.tableName = name
		}
	}
}
