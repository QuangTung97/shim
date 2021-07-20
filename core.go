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

type nodeState struct {
	status nodeStatus
	addr   string
	leftAt time.Time
}

type state struct {
	nodes      map[string]nodeState
	partitions []partitionState
}

type coreService struct {
	mut sync.Mutex

	partitionCount int
	options        options
	selfNode       string
	runner         PartitionRunner
	nodeDelegate   NodeDelegate

	staticAddrs  []string
	finishedJoin bool
	nodes        map[string]string
}

func newCoreService(
	partitionCount int, selfNode string,
	runner PartitionRunner, nodeDelegate NodeDelegate,
	opts options,
) *coreService {
	return &coreService{
		partitionCount: partitionCount,
		options:        opts,
		selfNode:       selfNode,
		runner:         runner,
		nodeDelegate:   nodeDelegate,

		staticAddrs: opts.staticAddrs,
		nodes:       map[string]string{},
	}
}

func updateStaticAddresses(configured []string, selfAddr string) []string {
	var result []string
	for _, a := range configured {
		if a == selfAddr {
			continue
		}
		result = append(result, a)
	}
	return result
}

func (s *coreService) nodeJoin(name string, addr string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.nodes[name] = addr

	if name == s.selfNode {
		s.staticAddrs = updateStaticAddresses(s.staticAddrs, addr)
		if len(s.staticAddrs) > 0 {
			s.nodeDelegate.Join(s.staticAddrs, s.finishJoin)
			return
		}

		for p := 0; p < s.partitionCount; p++ {
			s.runner.Start(PartitionID(p), nil)
		}
	}
}

func (s *coreService) finishJoin() {
	s.mut.Lock()
	defer s.mut.Unlock()

	nodes := make([]string, 0, len(s.nodes))
	for name := range s.nodes {
		nodes = append(nodes, name)
	}
	sort.Strings(nodes)

	expected := reallocatePartitions(s.partitionCount, nodes, nil)
	for _, p := range expected[s.selfNode] {
		s.runner.Start(p, nil)
	}
}
