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
