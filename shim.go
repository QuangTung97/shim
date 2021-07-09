package shim

// PartitionID ...
type PartitionID uint32

//go:generate moq -out shim_mocks_test.go . PartitionRunner NodeDelegate

// PartitionRunner ...
type PartitionRunner interface {
	Start(partition PartitionID, finish func())
	Stop()
}

// NodeDelegate ...
type NodeDelegate interface {
	Join(addrs []string, finish func())
	Lease()
}

// Timer ...
type Timer interface {
	Reset()
	Stop()
}
