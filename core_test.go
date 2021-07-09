package shim

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCoreService_OnlyNode(t *testing.T) {
	runner := &PartitionRunnerMock{}
	s := newCoreService(3, "A", runner, nil, computeOptions())

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
	delegate := &NodeDelegateMock{}
	s := newCoreService(3, "A", runner, delegate,
		computeOptions(WithStaticAddresses([]string{"address2", "address3"})),
	)

	var joinAddrs []string
	var finishFn func()
	delegate.JoinFunc = func(addrs []string, finish func()) {
		joinAddrs = addrs
		finishFn = finish
	}

	s.nodeJoin("A", "address1")
	assert.Equal(t, 0, len(runner.StartCalls()))
	assert.Equal(t, 1, len(delegate.JoinCalls()))

	s.nodeJoin("B", "address2")
	assert.Equal(t, 0, len(runner.StartCalls()))
	assert.Equal(t, []string{"address2", "address3"}, joinAddrs)

	var parts []PartitionID
	runner.StartFunc = func(partition PartitionID, finish func()) {
		parts = append(parts, partition)
	}

	finishFn()

	assert.Equal(t, 2, len(runner.StartCalls()))
	assert.Equal(t, []PartitionID{0, 1}, parts)
}

func TestCoreService_WithSelfNode(t *testing.T) {
	runner := &PartitionRunnerMock{}
	s := newCoreService(3, "A", runner, nil,
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

func TestCoreService_WithOtherNodes_5_Partitions(t *testing.T) {
	runner := &PartitionRunnerMock{}
	delegate := &NodeDelegateMock{}
	s := newCoreService(5, "A", runner, delegate,
		computeOptions(WithStaticAddresses([]string{"address1", "address2", "address3"})),
	)

	var joinAddrs []string
	var finishFn func()
	delegate.JoinFunc = func(addrs []string, finish func()) {
		joinAddrs = addrs
		finishFn = finish
	}

	s.nodeJoin("A", "address1")
	s.nodeJoin("B", "address2")
	s.nodeJoin("C", "address3")

	assert.Equal(t, []string{"address2", "address3"}, joinAddrs)
	assert.Equal(t, 1, len(delegate.JoinCalls()))

	assert.Equal(t, 0, len(runner.StartCalls()))

	var parts []PartitionID
	runner.StartFunc = func(partition PartitionID, finish func()) {
		parts = append(parts, partition)
	}

	finishFn()

	assert.Equal(t, 2, len(runner.StartCalls()))
	assert.Equal(t, []PartitionID{0, 1}, parts)
}
