package shim

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPartition_UpdateOwner_To_Self_Node__Start(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {
	}

	p.updateOwner("self-node")

	assert.Equal(t, partitionState{
		status: partitionStatusStarting,
		owner:  "self-node",
	}, p.state)

	assert.Equal(t, 1, len(delegate.startCalls()))
}

func TestPartition_CompleteStarting_Broadcast(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	var broadcastMsg partitionMsg
	delegate.broadcastFunc = func(msg partitionMsg) {
		broadcastMsg = msg
	}

	p.completeStarting()

	assert.Equal(t, 1, len(delegate.broadcastCalls()))
	assert.Equal(t, partitionMsg{
		current:     "self-node",
		incarnation: 1,
	}, broadcastMsg)
	assert.Equal(t, partitionState{
		status:      partitionStatusRunning,
		owner:       "self-node",
		current:     "self-node",
		incarnation: 1,
	}, p.state)
}

func TestPartition_CompleteStarting_With_Status_Not_Starting__Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.completeStarting()

	assert.Equal(t, 0, len(delegate.broadcastCalls()))
	assert.Equal(t, partitionState{
		status: partitionStatusStopped,
	}, p.state)
}

func TestPartition_UpdateOwner_To_Other_Node__Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.updateOwner("other-node")

	assert.Equal(t, partitionState{
		owner: "other-node",
	}, p.state)
	assert.Equal(t, 0, len(delegate.startCalls()))
}

func TestPartition_RecvBroadcast__Update_Current(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	assert.Equal(t, partitionState{
		owner:       "",
		current:     "other-node",
		incarnation: 2,
	}, p.state)
	assert.Equal(t, 0, len(delegate.startCalls()))
}

func TestPartition_RecvBroadcast_Then_Update_Owner__Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.updateOwner("self-node")

	assert.Equal(t, 0, len(delegate.startCalls()))
	assert.Equal(t, partitionState{
		owner:       "self-node",
		current:     "other-node",
		incarnation: 2,
	}, p.state)
}

func TestPartition_Recv_Older_Broadcast_Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.recvBroadcast(partitionMsg{
		current:     "other-node-2",
		incarnation: 1,
	})

	p.recvBroadcast(partitionMsg{
		current:     "other-node-3",
		incarnation: 1,
	})

	assert.Equal(t, partitionState{
		owner:       "",
		current:     "other-node",
		incarnation: 2,
	}, p.state)
}

func TestPartition_Node_Leave__Same_Node(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.nodeLeave("other-node")

	delegate.startFunc = func() {}

	p.updateOwner("self-node")

	assert.Equal(t, partitionState{
		status:      partitionStatusStarting,
		owner:       "self-node",
		current:     "other-node",
		left:        true,
		incarnation: 2,
	}, p.state)
	assert.Equal(t, 1, len(delegate.startCalls()))
}

func TestPartition_Node_Leave_Other_Node__Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.nodeLeave("other-node-xx")

	p.updateOwner("self-node")

	assert.Equal(t, partitionState{
		status:      partitionStatusStopped,
		owner:       "self-node",
		current:     "other-node",
		incarnation: 2,
	}, p.state)
}

func TestPartition_Node_Leave_Then_Recv_Broadcast(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.nodeLeave("other-node")

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	assert.Equal(t, partitionState{
		status:      partitionStatusStopped,
		owner:       "",
		current:     "other-node",
		left:        true,
		incarnation: 2,
	}, p.state)
}

func TestPartition_Recv_Broadcast_Update_Owner_Then_Leave(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.updateOwner("self-node")

	delegate.startFunc = func() {}

	p.nodeLeave("other-node")

	assert.Equal(t, partitionState{
		status:      partitionStatusStarting,
		owner:       "self-node",
		current:     "other-node",
		left:        true,
		incarnation: 2,
	}, p.state)
	assert.Equal(t, 1, len(delegate.startCalls()))
}

func TestPartition_Recv_Broadcast_Update_Owner_Then_Leave_Second_Times(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.updateOwner("self-node")

	delegate.startFunc = func() {}

	p.nodeLeave("other-node")
	p.nodeLeave("other-node")

	assert.Equal(t, partitionState{
		status:      partitionStatusStarting,
		owner:       "self-node",
		incarnation: 2,
		current:     "other-node",
		left:        true,
	}, p.state)
	assert.Equal(t, 1, len(delegate.startCalls()))
}

func TestPartition_Recv_Broadcast_Higher_Name(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node-01",
		incarnation: 2,
	})

	p.recvBroadcast(partitionMsg{
		current:     "other-node-02",
		incarnation: 2,
	})

	assert.Equal(t, partitionState{
		owner:       "",
		current:     "other-node-02",
		incarnation: 2,
	}, p.state)
}

func TestPartition_Recv_Broadcast_Then_Running(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.recvBroadcast(partitionMsg{
		current:     "other-node",
		incarnation: 2,
	})

	p.nodeLeave("other-node")

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	var broadcastMsg partitionMsg
	delegate.broadcastFunc = func(msg partitionMsg) {
		broadcastMsg = msg
	}
	p.completeStarting()

	assert.Equal(t, 1, len(delegate.broadcastCalls()))
	assert.Equal(t, partitionMsg{
		current:     "self-node",
		incarnation: 3,
	}, broadcastMsg)
	assert.Equal(t, partitionState{
		status:      partitionStatusRunning,
		owner:       "self-node",
		current:     "self-node",
		incarnation: 3,
		left:        false,
	}, p.state)
}

func TestPartition_Change_Owner_When_Starting__Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	p.updateOwner("other-node")

	assert.Equal(t, 1, len(delegate.startCalls()))
	assert.Equal(t, partitionState{
		status: partitionStatusStarting,
		owner:  "other-node",
	}, p.state)
}

func TestPartition_Change_Owner_When_Running(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	delegate.broadcastFunc = func(msg partitionMsg) {}
	p.completeStarting()

	delegate.stopFunc = func() {}
	p.updateOwner("other-node")

	assert.Equal(t, 1, len(delegate.startCalls()))
	assert.Equal(t, 1, len(delegate.stopCalls()))

	assert.Equal(t, partitionState{
		status:      partitionStatusStopping,
		owner:       "other-node",
		current:     "self-node",
		incarnation: 1,
	}, p.state)
}

func TestPartition_CompleteStopping(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	delegate.broadcastFunc = func(msg partitionMsg) {}
	p.completeStarting()

	delegate.stopFunc = func() {}
	p.updateOwner("other-node")

	var broadcastMsg partitionMsg
	delegate.broadcastFunc = func(msg partitionMsg) {
		broadcastMsg = msg
	}
	p.completeStopping()

	assert.Equal(t, partitionState{
		status:      partitionStatusStopped,
		owner:       "other-node",
		current:     "self-node",
		left:        true,
		incarnation: 1,
	}, p.state)
	assert.Equal(t, 2, len(delegate.broadcastCalls()))
	assert.Equal(t, partitionMsg{
		current:     "self-node",
		incarnation: 1,
		left:        true,
	}, broadcastMsg)
}

func TestPartition_Update_Owner_Back__Then_CompleteStopping(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	delegate.broadcastFunc = func(msg partitionMsg) {}
	p.completeStarting()

	delegate.stopFunc = func() {}
	p.updateOwner("other-node")

	p.updateOwner("self-node")

	delegate.broadcastFunc = func(msg partitionMsg) {}
	p.completeStopping()

	assert.Equal(t, partitionState{
		status:      partitionStatusStarting,
		owner:       "self-node",
		current:     "self-node",
		left:        true,
		incarnation: 1,
	}, p.state)

	assert.Equal(t, 2, len(delegate.broadcastCalls()))
	assert.Equal(t, 2, len(delegate.startCalls()))
}

func TestPartition_CompleteStopping_Not_Stopping__Do_Nothing(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	p.completeStopping()

	assert.Equal(t, partitionState{}, p.state)
}

func TestPartition_GetPartitionMsg__Init(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	msg := p.getPartitionMsg()
	assert.Equal(t, partitionMsg{
		incarnation: 0,
		current:     "",
		left:        false,
	}, msg)
}

func TestPartition_GetPartitionMsg__WhenRunning(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	assert.Equal(t, partitionMsg{
		incarnation: 0,
		current:     "",
		left:        false,
	}, p.getPartitionMsg())

	delegate.broadcastFunc = func(msg partitionMsg) {}
	p.completeStarting()

	assert.Equal(t, partitionMsg{
		incarnation: 1,
		current:     "self-node",
		left:        false,
	}, p.getPartitionMsg())
}

func TestPartition_GetPartitionMsg__When_Stopped(t *testing.T) {
	t.Parallel()

	delegate := &partitionDelegateMock{}
	p := newPartition("self-node", delegate)

	delegate.startFunc = func() {}
	p.updateOwner("self-node")

	p.updateOwner("other-node")

	delegate.stopFunc = func() {}
	delegate.broadcastFunc = func(msg partitionMsg) {}
	p.completeStarting()

	assert.Equal(t, 1, len(delegate.stopCalls()))
	assert.Equal(t, 1, len(delegate.broadcastCalls()))

	assert.Equal(t, partitionMsg{
		current:     "self-node",
		incarnation: 1,
		left:        false,
	}, p.getPartitionMsg())

	p.completeStopping()
	assert.Equal(t, 2, len(delegate.broadcastCalls()))

	assert.Equal(t, partitionMsg{
		current:     "self-node",
		incarnation: 1,
		left:        true,
	}, p.getPartitionMsg())
}

func TestPartitionState_UpdateByMsg(t *testing.T) {
	table := []struct {
		name     string
		prev     partitionState
		msg      partitionMsg
		expected partitionState
	}{
		{
			name: "from-empty",
			prev: partitionState{},
			msg: partitionMsg{
				incarnation: 3,
				current:     "node01",
			},
			expected: partitionState{
				incarnation: 3,
				current:     "node01",
			},
		},
		{
			name: "from-empty-with-left",
			prev: partitionState{},
			msg: partitionMsg{
				incarnation: 3,
				current:     "node01",
				left:        true,
			},
			expected: partitionState{
				incarnation: 3,
				current:     "node01",
				left:        true,
			},
		},
		{
			name: "higher-incarnation",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
			},
			msg: partitionMsg{
				incarnation: 5,
				current:     "node02",
			},
			expected: partitionState{
				incarnation: 5,
				current:     "node02",
			},
		},
		{
			name: "higher-incarnation-with-left",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
			msg: partitionMsg{
				incarnation: 5,
				current:     "node02",
			},
			expected: partitionState{
				incarnation: 5,
				current:     "node02",
			},
		},
		{
			name: "lower-incarnation",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
			},
			msg: partitionMsg{
				incarnation: 3,
				current:     "node04",
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node03",
			},
		},
		{
			name: "same-incarnation-lower-name",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
			},
			msg: partitionMsg{
				incarnation: 4,
				current:     "node02",
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node03",
			},
		},
		{
			name: "same-incarnation-higher-name",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
			},
			msg: partitionMsg{
				incarnation: 4,
				current:     "node05",
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node05",
			},
		},
		{
			name: "same-incarnation-higher-name-with-left",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
			msg: partitionMsg{
				incarnation: 4,
				current:     "node05",
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node05",
			},
		},
		{
			name: "same-incarnation-same-name-left-before",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
			msg: partitionMsg{
				incarnation: 4,
				current:     "node03",
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
		},
		{
			name: "same-incarnation-greater-name-left-before",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
			msg: partitionMsg{
				incarnation: 4,
				current:     "node05",
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node05",
			},
		},
		{
			name: "bigger-incarnation-left-before",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
			msg: partitionMsg{
				incarnation: 5,
				current:     "node03",
			},
			expected: partitionState{
				incarnation: 5,
				current:     "node03",
			},
		},
		{
			name: "same-incarnation-same-name-left-after",
			prev: partitionState{
				incarnation: 4,
				current:     "node03",
			},
			msg: partitionMsg{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
			expected: partitionState{
				incarnation: 4,
				current:     "node03",
				left:        true,
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			s := e.prev
			s.updateByMsg(e.msg)
			assert.Equal(t, e.expected, s)
		})
	}
}

func TestReallocatePartitions(t *testing.T) {
	table := []struct {
		name     string
		count    int
		nodes    []string
		current  partitionAssigns
		expected partitionAssigns
	}{
		{
			name:    "3-parts-current-nil",
			count:   3,
			nodes:   []string{"A", "B"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0, 1},
				"B": {2},
			},
		},
		{
			name:    "5-parts-current-nil",
			count:   5,
			nodes:   []string{"A", "B"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0, 1, 2},
				"B": {3, 4},
			},
		},
		{
			name:    "5-parts-single-node-current-nil",
			count:   5,
			nodes:   []string{"A"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0, 1, 2, 3, 4},
			},
		},
		{
			name:    "5-parts-more-nodes-than-partitions",
			count:   5,
			nodes:   []string{"A", "B", "C", "D", "E", "F"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0}, "B": {1}, "C": {2},
				"D": {3}, "E": {4}, "F": nil,
			},
		},
		{
			name:  "5-current-single-node",
			count: 5,
			nodes: []string{"A", "B"},
			current: map[string][]PartitionID{
				"A": {3},
			},
			expected: map[string][]PartitionID{
				"A": {3, 0, 1},
				"B": {2, 4},
			},
		},
		{
			name:  "5-current-single-node-with-more-partitions",
			count: 5,
			nodes: []string{"A", "B"},
			current: map[string][]PartitionID{
				"A": {3, 0, 2, 4},
			},
			expected: map[string][]PartitionID{
				"A": {3, 0, 2},
				"B": {1, 4},
			},
		},
		{
			name:  "7-parts-current-single-node-with-more-partitions",
			count: 7,
			nodes: []string{"A", "B", "C"},
			current: map[string][]PartitionID{
				"A": {6, 3, 2, 5},
				"C": {0, 1, 4},
			},
			expected: map[string][]PartitionID{
				"A": {6, 3, 2},
				"B": {4, 5},
				"C": {0, 1},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := reallocatePartitions(e.count, e.nodes, e.current)
			assert.Equal(t, e.expected, result)
		})
	}
}
