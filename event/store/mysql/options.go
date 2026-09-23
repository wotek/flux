package mysql

// Option configures a MySQL [EventStore].
type Option func(*config)

type config struct {
	tableName string
}

func defaultConfig() config {
	return config{
		tableName: "events",
	}
}

// WithTableName sets a custom table name for the events table.
func WithTableName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.tableName = name
		}
	}
}
