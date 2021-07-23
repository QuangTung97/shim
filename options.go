package shim

type serviceOptions struct {
	staticAddrs []string
}

// Option ...
type Option func(opts *serviceOptions)

func computeOptions(opts ...Option) serviceOptions {
	result := serviceOptions{}
	for _, o := range opts {
		o(&result)
	}
	return result
}

// WithStaticAddresses ...
func WithStaticAddresses(addrs []string) Option {
	return func(opts *serviceOptions) {
		opts.staticAddrs = addrs
	}
}
