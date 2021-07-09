package shim

import (
	"sort"
	"sync"
)

type coreService struct {
	mut sync.Mutex

	partitionCount int
	options        options
	selfNode       string
	runner         PartitionRunner

	staticAddrs        []string
	needFinishPushPull bool
	nodes              map[string]string
}

func newCoreService(partitionCount int, selfNode string, runner PartitionRunner, opts options) *coreService {
	return &coreService{
		partitionCount: partitionCount,
		options:        opts,
		selfNode:       selfNode,
		runner:         runner,

		staticAddrs: opts.staticAddrs,
		nodes:       map[string]string{},
	}
}

func (s *coreService) nodeJoin(name string, addr string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.nodes[name] = addr
	if name == s.selfNode {
		var newStaticAddrs []string
		for _, a := range s.staticAddrs {
			if a == addr {
				continue
			}
			newStaticAddrs = append(newStaticAddrs, a)
		}
		s.staticAddrs = newStaticAddrs
		if len(s.staticAddrs) > 0 {
			s.needFinishPushPull = true
		}
	}

	if s.needFinishPushPull {
		return
	}

	for p := 0; p < s.partitionCount; p++ {
		s.runner.Start(PartitionID(p), nil)
	}
}

func (s *coreService) finishPushPull() {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.needFinishPushPull = false

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
