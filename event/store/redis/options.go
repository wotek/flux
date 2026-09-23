package redis

// Option configures a Redis [EventStore].
type Option func(*config)

type config struct {
	keyPrefix string
	batchSize int64
}

func defaultConfig() config {
	return config{
		keyPrefix: "",
		batchSize: 100,
	}
}

// WithKeyPrefix sets a namespace prefix for all Redis keys created by the [EventStore].
func WithKeyPrefix(prefix string) Option {
	return func(c *config) {
		c.keyPrefix = prefix
	}
}

// WithBatchSize sets the number of stream entries fetched per read call.
func WithBatchSize(size int64) Option {
	return func(c *config) {
		if size > 0 {
			c.batchSize = size
		}
	}
}
