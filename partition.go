package shim

type partitionStatus int

const (
	partitionStatusStopped partitionStatus = iota
	partitionStatusStarting
	partitionStatusRunning
	partitionStatusStopping
)

type partitionState struct {
	status       partitionStatus
	owner        string
	shuttingDown bool

	incarnation uint64
	current     string
	left        bool
}

type partitionMsg struct {
	incarnation uint64
	current     string
	left        bool
}

//go:generate moq -out partition_mocks_test.go . partitionDelegate

type partitionDelegate interface {
	start()
	stop()
	broadcast(msg partitionMsg)
}

type partition struct {
	self     string
	delegate partitionDelegate
	state    partitionState
}

func newPartition(selfName string, delegate partitionDelegate) partition {
	return partition{
		self:     selfName,
		delegate: delegate,
		state:    partitionState{},
	}
}

func (p *partition) handleStateChanged() {
	switch p.state.status {
	case partitionStatusStopped:
		p.handleStateChangedWhenStopped()
	case partitionStatusRunning:
		p.handleStateChangedWhenRunning()
	default:
	}
}

func (p *partition) handleStateChangedWhenStopped() {
	if p.state.owner != p.self {
		return
	}

	if p.state.current != "" && !p.state.left {
		return
	}

	p.state.status = partitionStatusStarting
	p.delegate.start()
}

func (p *partition) handleStateChangedWhenRunning() {
	if p.state.owner == p.self {
		return
	}

	p.state.status = partitionStatusStopping
	p.delegate.stop()
}

func (p *partition) updateOwner(owner string) {
	defer p.handleStateChanged()

	p.state.owner = owner
}

func (p *partition) completeStarting() {
	defer p.handleStateChanged()

	if p.state.status != partitionStatusStarting {
		return
	}

	p.state.incarnation++
	p.state.status = partitionStatusRunning
	p.state.current = p.self
	p.state.left = false

	p.delegate.broadcast(p.getPartitionMsg())
}

func (p *partition) completeStopping() {
	defer p.handleStateChanged()

	if p.state.status != partitionStatusStopping {
		return
	}

	p.state.status = partitionStatusStopped
	p.state.left = true

	p.delegate.broadcast(p.getPartitionMsg())
}

func (p *partition) recvBroadcast(msg partitionMsg) {
	defer p.handleStateChanged()

	p.state.updateByMsg(msg)
}

func (p *partition) nodeLeave(name string) {
	defer p.handleStateChanged()

	if p.state.current == name {
		p.state.left = true
	}
}

func (p *partition) getPartitionMsg() partitionMsg {
	return partitionMsg{
		incarnation: p.state.incarnation,
		current:     p.state.current,
		left:        p.state.left,
	}
}

func (s *partitionState) setCurrentState(msg partitionMsg) {
	s.current = msg.current
	s.incarnation = msg.incarnation
	s.left = msg.left
}

func (s *partitionState) updateByMsg(msg partitionMsg) {
	if s.incarnation > msg.incarnation {
		return
	}

	if s.incarnation < msg.incarnation {
		s.setCurrentState(msg)
		return
	}

	if s.current > msg.current {
		return
	}

	if s.current < msg.current {
		s.setCurrentState(msg)
		return
	}

	if s.left {
		return
	}
	s.left = msg.left
}

type partitionAssigns map[string][]PartitionID

func reallocatePartitions(count int, nodes []string, current partitionAssigns) partitionAssigns {
	high := (count + len(nodes) - 1) / len(nodes)
	low := count / len(nodes)
	highCount := count - low*len(nodes)

	allocatedPartitions := make([]bool, count)

	allocated := make([][]PartitionID, len(nodes))
	for i, node := range nodes {
		numPartitions := low
		if i < highCount {
			numPartitions = high
		}

		n := len(current[node])
		if n > numPartitions {
			n = numPartitions
		}

		allocated[i] = current[node][:n]
		for _, p := range allocated[i] {
			allocatedPartitions[p] = true
		}
	}

	var freePartitions []PartitionID
	for p, used := range allocatedPartitions {
		if !used {
			freePartitions = append(freePartitions, PartitionID(p))
		}
	}

	result := partitionAssigns{}
	for i, node := range nodes {
		numPartitions := low
		if i < highCount {
			numPartitions = high
		}
		missing := numPartitions - len(allocated[i])

		result[node] = append(result[node], allocated[i]...)
		result[node] = append(result[node], freePartitions[:missing]...)
		freePartitions = freePartitions[missing:]
	}
	return result
}
