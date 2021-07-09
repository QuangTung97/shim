package shim

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCoreService_OnlyNode(t *testing.T) {
	runner := &PartitionRunnerMock{}
	s := newCoreService(3, "A", runner, computeOptions())

	var parts []PartitionID
	runner.StartFunc = func(partition PartitionID, finish func()) {
		parts = append(parts, partition)
	}

	s.nodeJoin("A", "address1")
	assert.Equal(t, 3, len(runner.StartCalls()))
	assert.Equal(t, []PartitionID{0, 1, 2}, parts)
}

func TestCoreService_WithOtherNodes(t *testing.T) {
	runner := &PartitionRunnerMock{}
	s := newCoreService(3, "A", runner,
		computeOptions(WithStaticAddresses([]string{"address2", "address3"})),
	)

	s.nodeJoin("A", "address1")
	assert.Equal(t, 0, len(runner.StartCalls()))
	s.nodeJoin("B", "address2")
	assert.Equal(t, 0, len(runner.StartCalls()))

	var parts []PartitionID
	runner.StartFunc = func(partition PartitionID, finish func()) {
		parts = append(parts, partition)
	}

	s.finishPushPull()

	assert.Equal(t, 2, len(runner.StartCalls()))
	assert.Equal(t, []PartitionID{0, 1}, parts)
}

func TestCoreService_WithSelfNode(t *testing.T) {
	runner := &PartitionRunnerMock{}
	s := newCoreService(3, "A", runner,
		computeOptions(WithStaticAddresses([]string{"address1"})),
	)

	var parts []PartitionID
	runner.StartFunc = func(partition PartitionID, finish func()) {
		parts = append(parts, partition)
	}

	s.nodeJoin("A", "address1")

	assert.Equal(t, 3, len(runner.StartCalls()))
	assert.Equal(t, []PartitionID{0, 1, 2}, parts)
}
