package shim

type sequenceManager struct {
	first     uint64
	last      uint64
	sequences []bool
}

func newSequenceManager(size int) *sequenceManager {
	return &sequenceManager{
		first:     1,
		last:      1,
		sequences: make([]bool, size),
	}
}

func (m *sequenceManager) completed(seq uint64) bool {
	if seq >= m.last {
		return false
	}
	if seq < m.first {
		return true
	}
	return m.sequences[seq%m.size()]
}

func (m *sequenceManager) size() uint64 {
	return uint64(len(m.sequences))
}

func (m *sequenceManager) clear(seq uint64) {
	index := seq % m.size()
	m.sequences[index] = false
}

func (m *sequenceManager) clearTo(to uint64) {
	seq := m.first
	for ; seq < to; seq++ {
		m.clear(seq)
	}
}

func (m *sequenceManager) clearAndSetFirst() {
	from := m.first
	if m.last > m.first+m.size() {
		from = m.last - m.size()
	}

	seq := from
	for ; seq < m.last; seq++ {
		if m.sequences[seq%m.size()] == false {
			break
		}
	}

	m.clearTo(seq)
	m.first = seq
}

func (m *sequenceManager) setCompleted(seq uint64) {
	if m.last <= seq {
		m.last = seq + 1
	}

	m.clearAndSetFirst()
	m.sequences[seq%m.size()] = true
	m.clearAndSetFirst()
}

func (m *sequenceManager) setAllCompleted(seq uint64) {
	for i := range m.sequences {
		m.sequences[i] = false
	}
	m.first = seq + 1
	m.last = seq + 1
}

func (m *sequenceManager) uncompletedFrom() uint64 {
	return m.first
}
