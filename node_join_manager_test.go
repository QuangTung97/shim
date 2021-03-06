package shim

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNodeJoinManager_WithStaticAddrs(t *testing.T) {
	m := newNodeJoinManager(
		"self-node", "address01", nil, nil,
		computeOptions(WithStaticAddresses([]string{"address01", "address02", "address03"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string{"address02", "address03"}, joinAddrs)
	assert.Equal(t, uint64(0), version)
}

func TestNodeJoinManager_WithStaticAddrs_Itself(t *testing.T) {
	m := newNodeJoinManager(
		"self-node", "address01", nil, nil,
		computeOptions(WithStaticAddresses([]string{"address01"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(0), version)
}

func TestNodeJoinManager_WithNoStaticAddrs(t *testing.T) {
	m := newNodeJoinManager(
		"self-node", "address01", nil, nil,
		computeOptions(WithStaticAddresses(nil)),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(0), version)
}

func TestNodeJoinManager_Join_Completed_All_Nodes(t *testing.T) {
	listener := &nodeListenerMock{}
	m := newNodeJoinManager(
		"self-node", "address01", listener, nil,
		computeOptions(WithStaticAddresses([]string{"address02", "address03"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string{"address02", "address03"}, joinAddrs)
	assert.Equal(t, uint64(0), version)

	var listenNodes []nodeInfo
	listener.onChangeFunc = func(nodes []nodeInfo) { listenNodes = nodes }
	m.notifyJoin("other02", "address02")

	assert.Equal(t, 1, len(listener.onChangeCalls()))
	assert.Equal(t, []nodeInfo{
		{name: "other02", addr: "address02"},
		{name: "self-node", addr: "address01"},
	}, listenNodes)

	joinAddrs, version = m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(1), version)

	m.notifyJoin("other01", "address03")

	assert.Equal(t, 2, len(listener.onChangeCalls()))
	assert.Equal(t, []nodeInfo{
		{name: "other01", addr: "address03"},
		{name: "other02", addr: "address02"},
		{name: "self-node", addr: "address01"},
	}, listenNodes)

	joinAddrs, version = m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(2), version)

	listener.onJoinCompletedFunc = func() {}
	m.joinCompleted()

	assert.Equal(t, 1, len(listener.onJoinCompletedCalls()))
}

func TestNodeJoinManager_Join_Completed_Missing_Node(t *testing.T) {
	listener := &nodeListenerMock{}
	m := newNodeJoinManager(
		"self-node", "address01", listener, nil,
		computeOptions(WithStaticAddresses([]string{"address03", "address02"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string{"address02", "address03"}, joinAddrs)
	assert.Equal(t, uint64(0), version)

	var listenNodes []nodeInfo
	listener.onChangeFunc = func(nodes []nodeInfo) { listenNodes = nodes }
	m.notifyJoin("other01", "address02")

	assert.Equal(t, []nodeInfo{
		{name: "other01", addr: "address02"},
		{name: "self-node", addr: "address01"},
	}, listenNodes)

	joinAddrs, version = m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(1), version)

	listener.onJoinCompletedFunc = func() {}
	m.joinCompleted()

	assert.Equal(t, 1, len(listener.onJoinCompletedCalls()))

	joinAddrs, version = m.needJoin()
	assert.Equal(t, []string{"address03"}, joinAddrs)
	assert.Equal(t, uint64(1), version)
}

func TestNodeJoinManager_Notify_Left_Msg(t *testing.T) {
	listener := &nodeListenerMock{}
	broadcast := &nodeBroadcasterMock{}

	m := newNodeJoinManager(
		"self-node", "address01", listener, broadcast,
		computeOptions(WithStaticAddresses([]string{"address03", "address02"})),
	)

	m.needJoin()

	listener.onChangeFunc = func(nodes []nodeInfo) {}

	m.notifyJoin("other01", "address02")
	m.notifyJoin("other02", "address03")

	listener.onJoinCompletedFunc = func() {}
	m.joinCompleted()

	var broadcastMsg nodeLeftMsg
	broadcast.broadcastFunc = func(msg nodeLeftMsg) {
		broadcastMsg = msg
	}

	var changeNodes []nodeInfo
	listener.onChangeFunc = func(nodes []nodeInfo) {
		changeNodes = nodes
	}
	m.notifyMsg(nodeLeftMsg{
		name: "other01",
		addr: "address02",
	})

	assert.Equal(t, 3, len(listener.onChangeCalls()))
	assert.Equal(t, []nodeInfo{
		{name: "other02", addr: "address03"},
		{name: "self-node", addr: "address01"},
	}, changeNodes)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, uint64(3), version)
	assert.Equal(t, []string(nil), joinAddrs)

	assert.Equal(t, 1, len(broadcast.broadcastCalls()))
	assert.Equal(t, nodeLeftMsg{
		name: "other01",
		addr: "address02",
	}, broadcastMsg)
}

func TestNodeJoinManager_Notify_Left_Msg_Second_Time(t *testing.T) {
	listener := &nodeListenerMock{}
	broadcast := &nodeBroadcasterMock{}

	m := newNodeJoinManager(
		"self-node", "address01", listener, broadcast,
		computeOptions(WithStaticAddresses([]string{"address03", "address02"})),
	)

	m.needJoin()

	listener.onChangeFunc = func(nodes []nodeInfo) {}

	m.notifyJoin("other01", "address02")
	m.notifyJoin("other02", "address03")

	listener.onJoinCompletedFunc = func() {}
	m.joinCompleted()

	broadcast.broadcastFunc = func(msg nodeLeftMsg) {}
	m.notifyMsg(nodeLeftMsg{
		name: "other01",
		addr: "address02",
	})

	assert.Equal(t, 3, len(listener.onChangeCalls()))

	joinAddrs, version := m.needJoin()
	assert.Equal(t, uint64(3), version)
	assert.Equal(t, []string(nil), joinAddrs)

	assert.Equal(t, 1, len(broadcast.broadcastCalls()))

	m.notifyMsg(nodeLeftMsg{
		name: "other01",
		addr: "address02",
	})
	assert.Equal(t, 1, len(broadcast.broadcastCalls()))

	assert.Equal(t, 3, len(listener.onChangeCalls()))
}

func TestNodeJoinManager_Node_Leave(t *testing.T) {
	listener := &nodeListenerMock{}
	m := newNodeJoinManager(
		"self-node", "address01", listener, nil,
		computeOptions(WithStaticAddresses([]string{"address03", "address02"})),
	)

	m.needJoin()

	var changeNodes []nodeInfo
	listener.onChangeFunc = func(nodes []nodeInfo) {
		changeNodes = nodes
	}

	m.notifyJoin("other01", "address02")
	m.notifyJoin("other02", "address03")

	listener.onJoinCompletedFunc = func() {}
	m.joinCompleted()

	m.notifyLeave("other01")

	assert.Equal(t, 3, len(listener.onChangeCalls()))
	assert.Equal(t, []nodeInfo{
		{name: "other02", addr: "address03"},
		{name: "self-node", addr: "address01"},
	}, changeNodes)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, uint64(3), version)
	assert.Equal(t, []string{"address02"}, joinAddrs)
}

func TestNodeJoinManager_Notify_Left_Msg_And_Then_Leave__No_Need_Join(t *testing.T) {
	listener := &nodeListenerMock{}
	broadcaster := &nodeBroadcasterMock{}
	m := newNodeJoinManager(
		"self-node", "address01", listener, broadcaster,
		computeOptions(WithStaticAddresses([]string{"address03", "address02"})),
	)

	m.needJoin()

	listener.onChangeFunc = func(nodes []nodeInfo) {}

	m.notifyJoin("other01", "address02")
	m.notifyJoin("other02", "address03")

	listener.onJoinCompletedFunc = func() {}
	m.joinCompleted()

	broadcaster.broadcastFunc = func(msg nodeLeftMsg) {}
	m.notifyMsg(nodeLeftMsg{name: "other01", addr: "address02"})
	m.notifyLeave("other01")

	joinAddrs, version := m.needJoin()
	assert.Equal(t, uint64(4), version)
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, 1, len(broadcaster.broadcastCalls()))
}

func TestRemoveSelfAddrInConfiguredStaticAddrs(t *testing.T) {
	t.Run("existed", func(t *testing.T) {
		result := removeSelfAddrInConfiguredStaticAddrs([]string{
			"address01",
			"address02",
			"address03",
		}, "address02")
		assert.Equal(t, []string{
			"address01",
			"address03",
		}, result)
	})

	t.Run("not-existed", func(t *testing.T) {
		result := removeSelfAddrInConfiguredStaticAddrs([]string{
			"address01",
			"address02",
			"address03",
		}, "address04")
		assert.Equal(t, []string{
			"address01",
			"address02",
			"address03",
		}, result)
	})
}

func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestNodeJoin(t *testing.T) {
	t.Run("from-empty", func(t *testing.T) {
		result := nodeJoin(nil, "node01", "address01",
			nil, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}, result)
	})

	t.Run("normal", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}

		result := nodeJoin(nodes, "node02", "address02",
			nil, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
			"node02": {
				status: nodeStatusAlive,
				addr:   "address02",
			},
		}, result)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}, nodes)
	})

	t.Run("left-nodes-with-same-addr-not-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address02",
			[]string{"address01", "address02"},
			mustParse("2021-07-26T10:00:29+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
			"node03": {
				addr: "address02",
			},
		}, result)

	})

	t.Run("left-nodes-with-same-addr-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address02",
			[]string{"address01", "address02"},
			mustParse("2021-07-26T10:00:30+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
			"node03": {
				addr: "address02",
			},
		}, result)
	})

	t.Run("left-nodes-with-same-addr-both-expired-but-still-exist-one", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address02",
			[]string{"address01", "address02"},
			mustParse("2021-07-26T10:00:40+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
			"node03": {
				addr: "address02",
			},
		}, result)

	})

	t.Run("left-nodes-not-in-configured-not-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address02",
			nil, mustParse("2021-07-26T10:00:27+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
			"node03": {
				addr: "address02",
			},
		}, result)
	})

	t.Run("left-nodes-not-in-configured-both-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address02",
			nil, mustParse("2021-07-26T10:00:31+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node03": {
				addr: "address02",
			},
		}, result)
	})

	t.Run("join-node-with-same-addr-not-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address01",
			[]string{"address01"}, mustParse("2021-07-26T10:00:29+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
			"node03": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}, result)
	})

	t.Run("join-node-with-same-addr-both-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
			"node02": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:01+07:00"),
			},
		}

		result := nodeJoin(nodes, "node03", "address01",
			[]string{"address01"}, mustParse("2021-07-26T10:00:31+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node03": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}, result)
	})
}

func TestNodeGracefulLeave(t *testing.T) {
	t.Run("from-empty", func(t *testing.T) {
		result, changed := nodeGracefulLeave(nil, "node01", "address01",
			[]string{"address01"}, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}, result)
		assert.Equal(t, true, changed)
	})

	t.Run("single-node-in-configured", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}
		result, changed := nodeGracefulLeave(nodes, "node01", "address01",
			[]string{"address01"}, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}, result)
		assert.Equal(t, true, changed)
	})

	t.Run("single-node-not-in-configured", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}
		result, changed := nodeGracefulLeave(nodes, "node01", "address01",
			nil, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}, result)
		assert.Equal(t, true, changed)
	})

	t.Run("not-changed", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}
		result, changed := nodeGracefulLeave(nodes, "node01", "address01",
			nil, mustParse("2021-07-26T10:00:20+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}, result)
		assert.Equal(t, false, changed)
	})
}

func TestNodeLeave(t *testing.T) {
	t.Run("from-empty", func(t *testing.T) {
		result := nodeLeave(nil, "node01",
			nil, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{}, result)
	})

	t.Run("single-node-alive", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusAlive,
				addr:   "address01",
			},
		}
		result := nodeLeave(nodes, "node01",
			[]string{"address01"}, mustParse("2021-07-26T10:00:00+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{}, result)
	})

	t.Run("single-node-graceful-left", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}
		result := nodeLeave(nodes, "node01",
			[]string{"address01"}, mustParse("2021-07-26T10:00:20+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}, result)
	})

	t.Run("single-node-graceful-left-not-in-config-expired", func(t *testing.T) {
		nodes := map[string]nodeState{
			"node01": {
				status: nodeStatusGracefulLeft,
				addr:   "address01",
				leftAt: mustParse("2021-07-26T10:00:00+07:00"),
			},
		}
		result := nodeLeave(nodes, "node01",
			nil, mustParse("2021-07-26T10:00:30+07:00"), 30*time.Second)

		assert.Equal(t, map[string]nodeState{}, result)
	})
}

func TestCompressNodeStates(t *testing.T) {
	table := []struct {
		name       string
		nodes      []addressNodeState
		configured bool
		now        time.Time
		expected   []addressNodeState
	}{
		{
			name: "single-alive",
			nodes: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusAlive,
				},
			},
			expected: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusAlive,
				},
			},
		},
		{
			name: "alive-and-left-not-expired",
			nodes: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusAlive,
				},
				{
					name:   "node02",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:00+07:00"),
				},
			},
			now: mustParse("2021-07-26T10:00:00+07:00"),
			expected: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusAlive,
				},
				{
					name:   "node02",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:00+07:00"),
				},
			},
		},
		{
			name: "alive-and-left-expired",
			nodes: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusAlive,
				},
				{
					name:   "node02",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:00+07:00"),
				},
			},
			now: mustParse("2021-07-26T10:00:30+07:00"),
			expected: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusAlive,
				},
			},
		},
		{
			name: "multiple-expired-not-configured",
			nodes: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:01+07:00"),
				},
				{
					name:   "node02",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:00+07:00"),
				},
			},
			now: mustParse("2021-07-26T10:00:40+07:00"),
		},
		{
			name: "multiple-expired-configured",
			nodes: []addressNodeState{
				{
					name:   "node01",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:01+07:00"),
				},
				{
					name:   "node02",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:02+07:00"),
				},
				{
					name:   "node03",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:04+07:00"),
				},
				{
					name:   "node04",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:03+07:00"),
				},
			},
			configured: true,
			now:        mustParse("2021-07-26T10:00:40+07:00"),
			expected: []addressNodeState{
				{
					name:   "node03",
					status: nodeStatusGracefulLeft,
					leftAt: mustParse("2021-07-26T10:00:04+07:00"),
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := compressAddressNodeStates(e.nodes, e.configured, e.now, 30*time.Second)
			assert.Equal(t, e.expected, result)
		})
	}
}
