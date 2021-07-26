package shim

import (
	"sort"
	"sync"
	"time"
)

type nodeStatus int

const (
	nodeStatusAlive nodeStatus = iota
	nodeStatusGracefulLeft
)

type nodeLeftMsg struct {
	name string
	addr string
}

type nodeState struct {
	status nodeStatus
	addr   string
	leftAt time.Time
}

type nodeJoinManager struct {
	mut sync.Mutex

	listener           nodeListener
	staticAddrs        []string
	gracefulLeftExpire time.Duration

	selfNode string
	selfAddr string

	joining bool
	version uint64
	nodes   map[string]nodeState

	getNow func() time.Time
}

func newNodeJoinManager(
	selfNode string, selfAddr string, listener nodeListener,
	opts serviceOptions,
) *nodeJoinManager {
	return &nodeJoinManager{
		listener:           listener,
		staticAddrs:        removeSelfAddrInConfiguredStaticAddrs(opts.staticAddrs, selfAddr),
		gracefulLeftExpire: 30 * time.Second,

		selfNode: selfNode,
		selfAddr: selfAddr,

		joining: false,
		version: 0,
		nodes:   map[string]nodeState{},

		getNow: func() time.Time { return time.Now() },
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

	m.version++
	m.nodes = nodeJoin(m.nodes, name, addr, m.staticAddrs, m.getNow(), m.gracefulLeftExpire)

	nodes := make([]string, 0, len(m.nodes)+1)
	nodes = append(nodes, m.selfNode)
	for n := range m.nodes {
		// TODO graceful left nodes
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	m.listener.onChange(nodes)
}

func (m *nodeJoinManager) notifyLeave(name string) {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.version++
	m.nodes = nodeLeave(m.nodes, name, m.staticAddrs, m.getNow(), m.gracefulLeftExpire)
}

func (m *nodeJoinManager) joinCompleted() {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.joining = false
	m.listener.onJoinCompleted()
}

func (m *nodeJoinManager) notifyMsg(msg nodeLeftMsg) {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.version++

	var changed bool
	m.nodes, changed = nodeGracefulLeave(m.nodes, msg.name, msg.addr, m.staticAddrs, m.getNow(), m.gracefulLeftExpire)

	if changed {
		// TODO
	}
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

type addressNodeState struct {
	name   string
	status nodeStatus
	leftAt time.Time
}

func compressAddressNodeStates(
	nodes []addressNodeState, configured bool,
	now time.Time, expire time.Duration,
) []addressNodeState {
	var filtered []addressNodeState
	for _, n := range nodes {
		if n.status == nodeStatusGracefulLeft {
			if !n.leftAt.Add(expire).After(now) {
				continue
			}
		}
		filtered = append(filtered, n)
	}

	if len(filtered) == 0 && configured {
		maxLeftAt := nodes[0].leftAt
		maxIndex := 0
		for i, n := range nodes[1:] {
			if n.leftAt.After(maxLeftAt) {
				maxLeftAt = n.leftAt
				maxIndex = i + 1
			}
		}

		maxNode := nodes[maxIndex]
		return []addressNodeState{maxNode}
	}

	return filtered
}

func computeKeptNodes(
	nodes map[string]nodeState,
	configured []string, now time.Time, expire time.Duration,
) map[string]nodeState {
	configuredSet := map[string]struct{}{}
	for _, a := range configured {
		configuredSet[a] = struct{}{}
	}

	addressMap := map[string][]addressNodeState{}
	for key, n := range nodes {
		addressMap[n.addr] = append(addressMap[n.addr], addressNodeState{
			name:   key,
			status: n.status,
			leftAt: n.leftAt,
		})
	}

	filteredAddressMap := map[string][]addressNodeState{}
	for addr, list := range addressMap {
		_, existed := configuredSet[addr]

		compressed := compressAddressNodeStates(list, existed, now, expire)
		if len(compressed) == 0 {
			continue
		}
		filteredAddressMap[addr] = compressed
	}

	result := map[string]nodeState{}
	for addr, list := range filteredAddressMap {
		for _, n := range list {
			result[n.name] = nodeState{
				status: n.status,
				addr:   addr,
				leftAt: n.leftAt,
			}
		}
	}
	return result
}

func cloneNodeStates(input map[string]nodeState) map[string]nodeState {
	nodes := map[string]nodeState{}
	for k, v := range input {
		nodes[k] = v
	}
	return nodes
}

func nodeJoin(
	inputNodes map[string]nodeState, name string, addr string,
	configured []string, now time.Time, expire time.Duration,
) map[string]nodeState {
	nodes := cloneNodeStates(inputNodes)
	nodes[name] = nodeState{
		status: nodeStatusAlive,
		addr:   addr,
	}

	return computeKeptNodes(nodes, configured, now, expire)
}

func nodeGracefulLeave(
	inputNodes map[string]nodeState, name string, addr string,
	configured []string, now time.Time, expire time.Duration,
) (map[string]nodeState, bool) {
	nodes := cloneNodeStates(inputNodes)

	prev, ok := nodes[name]
	if ok && prev.status == nodeStatusGracefulLeft {
		return computeKeptNodes(nodes, configured, now, expire), false
	}

	nodes[name] = nodeState{
		status: nodeStatusGracefulLeft,
		addr:   addr,
		leftAt: now,
	}
	return computeKeptNodes(nodes, configured, now, expire), true
}

func nodeLeave(
	inputNodes map[string]nodeState, name string,
	configured []string, now time.Time, expire time.Duration,
) map[string]nodeState {
	nodes := cloneNodeStates(inputNodes)
	node, ok := nodes[name]
	if ok && node.status == nodeStatusAlive {
		delete(nodes, name)
	}
	return computeKeptNodes(nodes, configured, now, expire)
}
