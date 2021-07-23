package shim

import (
	"sync"
)

type coreService struct {
	mut sync.Mutex

	partitionCount int
	options        serviceOptions
	selfNode       string
	runner         PartitionRunner

	partitions []partition
}

func newCoreService(
	partitionCount int, selfNode string,
	runner PartitionRunner, opts serviceOptions,
) *coreService {
	return &coreService{
		partitionCount: partitionCount,
		options:        opts,
		selfNode:       selfNode,
		runner:         runner,
	}
}
