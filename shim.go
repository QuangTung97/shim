package shim

// PartitionID ...
type PartitionID uint32

//go:generate moq -out shim_mocks_test.go . PartitionRunner NodeDelegate

// PartitionRunner ...
type PartitionRunner interface {
	Start(partition PartitionID, startCompleted func())
	Stop(partition PartitionID, stopCompleted func())
}

// NodeDelegate ...
type NodeDelegate interface {
	Join(addrs []string, finish func())
	Leave()
}

// Timer ...
type Timer interface {
	Reset()
	Stop()
}
