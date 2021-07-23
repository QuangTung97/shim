package shim

import (
	"sort"
	"sync"
	"time"
)

type nodeStatus int

const (
	nodeStatusAlive nodeStatus = iota
	nodeStatusDead
	nodeStatusLeft
)

type nodeMsg struct {
	name string
}

type nodeState struct {
	status nodeStatus
	addr   string
	leftAt time.Time
}

type nodeJoinManager struct {
	mut sync.Mutex

	listener    nodeListener
	staticAddrs []string

	selfNode string
	selfAddr string

	joining bool
	version uint64
	nodes   map[string]nodeState
}

func newNodeJoinManager(
	selfNode string, selfAddr string, listener nodeListener,
	opts serviceOptions,
) *nodeJoinManager {
	return &nodeJoinManager{
		listener:    listener,
		staticAddrs: removeSelfAddrInConfiguredStaticAddrs(opts.staticAddrs, selfAddr),

		selfNode: selfNode,
		selfAddr: selfAddr,

		joining: false,
		version: 0,
		nodes:   map[string]nodeState{},
	}
}

func (m *nodeJoinManager) needJoin() ([]string, uint64) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if m.joining {
		return nil, m.version
	}
	m.joining = true

	nodeAddrSet := map[string]struct{}{}
	for _, n := range m.nodes {
		nodeAddrSet[n.addr] = struct{}{}
	}

	var joinAddrs []string
	for _, a := range m.staticAddrs {
		_, existed := nodeAddrSet[a]
		if existed {
			continue
		}
		joinAddrs = append(joinAddrs, a)
	}
	sort.Strings(joinAddrs)
	return joinAddrs, m.version
}

func (m *nodeJoinManager) notifyJoin(name string, addr string) {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.nodes[name] = nodeState{
		status: nodeStatusAlive,
		addr:   addr,
	}
	m.version++

	nodes := make([]string, 0, len(m.nodes)+1)
	nodes = append(nodes, m.selfNode)
	for n := range m.nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	m.listener.onChange(nodes)
}

func (m *nodeJoinManager) joinCompleted() {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.joining = false
	m.listener.onJoinCompleted()
}

func removeSelfAddrInConfiguredStaticAddrs(configured []string, selfAddr string) []string {
	var result []string
	for _, a := range configured {
		if a == selfAddr {
			continue
		}
		result = append(result, a)
	}
	return result
}
