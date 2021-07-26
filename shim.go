package shim

// PartitionID ...
type PartitionID uint32

//go:generate moq -out shim_mocks_test.go . PartitionRunner NodeDelegate nodeListener nodeBroadcaster

// PartitionRunner ...
type PartitionRunner interface {
	Start(partition PartitionID, startCompleted func())
	Stop(partition PartitionID, stopCompleted func())
}

// NodeDelegate ...
type NodeDelegate interface {
	Join(addrs []string) error
	Leave()
}

type nodeInfo struct {
	name string
	addr string
}

type nodeListener interface {
	onChange(nodes []nodeInfo)
	onJoinCompleted()
}

type nodeBroadcaster interface {
	broadcast(msg nodeLeftMsg)
}

// Timer ...
type Timer interface {
	Reset()
	Stop()
}
