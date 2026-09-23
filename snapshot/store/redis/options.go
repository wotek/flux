package redis

// Option configures a Redis [SnapshotStore].
type Option func(*config)

type config struct {
	keyPrefix string
}

func defaultConfig() config {
	return config{
		keyPrefix: "",
	}
}

// WithKeyPrefix sets a namespace prefix for all Redis keys created by the [SnapshotStore].
func WithKeyPrefix(prefix string) Option {
	return func(c *config) {
		c.keyPrefix = prefix
	}
}
