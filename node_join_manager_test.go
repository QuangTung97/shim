package shim

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNodeJoinManager_WithStaticAddrs(t *testing.T) {
	m := newNodeJoinManager(
		"self-node", "address01", nil,
		computeOptions(WithStaticAddresses([]string{"address01", "address02", "address03"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string{"address02", "address03"}, joinAddrs)
	assert.Equal(t, uint64(0), version)
}

func TestNodeJoinManager_WithStaticAddrs_Itself(t *testing.T) {
	m := newNodeJoinManager(
		"self-node", "address01", nil,
		computeOptions(WithStaticAddresses([]string{"address01"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(0), version)
}

func TestNodeJoinManager_WithNoStaticAddrs(t *testing.T) {
	m := newNodeJoinManager(
		"self-node", "address01", nil,
		computeOptions(WithStaticAddresses(nil)),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(0), version)
}

func TestNodeJoinManager_Join_Completed_All_Nodes(t *testing.T) {
	listener := &nodeListenerMock{}
	m := newNodeJoinManager(
		"self-node", "address01", listener,
		computeOptions(WithStaticAddresses([]string{"address02", "address03"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string{"address02", "address03"}, joinAddrs)
	assert.Equal(t, uint64(0), version)

	var listenNodes []string
	listener.onChangeFunc = func(nodes []string) { listenNodes = nodes }
	m.notifyJoin("other02", "address02")

	assert.Equal(t, 1, len(listener.onChangeCalls()))
	assert.Equal(t, []string{"other02", "self-node"}, listenNodes)

	joinAddrs, version = m.needJoin()
	assert.Equal(t, []string(nil), joinAddrs)
	assert.Equal(t, uint64(1), version)

	m.notifyJoin("other01", "address03")

	assert.Equal(t, 2, len(listener.onChangeCalls()))
	assert.Equal(t, []string{"other01", "other02", "self-node"}, listenNodes)

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
		"self-node", "address01", listener,
		computeOptions(WithStaticAddresses([]string{"address03", "address02"})),
	)

	joinAddrs, version := m.needJoin()
	assert.Equal(t, []string{"address02", "address03"}, joinAddrs)
	assert.Equal(t, uint64(0), version)

	var listenNodes []string
	listener.onChangeFunc = func(nodes []string) { listenNodes = nodes }
	m.notifyJoin("other01", "address02")

	assert.Equal(t, []string{"other01", "self-node"}, listenNodes)

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
