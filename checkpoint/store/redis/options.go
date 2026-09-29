package redis

// Option configures a Redis [Store].
type Option func(*config)

type config struct {
	keyPrefix string
}

func defaultConfig() config {
	return config{
		keyPrefix: "",
	}
}

// WithKeyPrefix sets a namespace prefix for all Redis keys created by the [Store].
func WithKeyPrefix(prefix string) Option {
	return func(c *config) {
		c.keyPrefix = prefix
	}
}
