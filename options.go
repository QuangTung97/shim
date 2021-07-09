package shim

type options struct {
	staticAddrs []string
}

// Option ...
type Option func(opts *options)

func computeOptions(opts ...Option) options {
	result := options{}
	for _, o := range opts {
		o(&result)
	}
	return result
}

// WithStaticAddresses ...
func WithStaticAddresses(addrs []string) Option {
	return func(opts *options) {
		opts.staticAddrs = addrs
	}
}
