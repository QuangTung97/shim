package shim

// PartitionID ...
type PartitionID uint32

// NodeName ...
type NodeName string

type partitionStatus int

const (
	partitionStatusInit partitionStatus = iota
	partitionStatusRunning
	partitionStatusWaitingFinish
)

type partitionState struct {
	status   partitionStatus
	current  NodeName
	expected NodeName
}

//go:generate moq -out shim_mocks_test.go . PartitionRunner

// PartitionRunner ...
type PartitionRunner interface {
	Start(partition PartitionID, finish func())
	Stop()
}

// Timer ...
type Timer interface {
	Reset()
	Stop()
}
