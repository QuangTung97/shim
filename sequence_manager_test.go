package shim

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSequenceManager_Simple(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(1))

	m.setCompleted(1)
	assert.Equal(t, true, m.completed(1))
}

func TestSequenceManager_In_Order(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(1))
	assert.Equal(t, false, m.completed(2))

	m.setCompleted(1)
	assert.Equal(t, true, m.completed(1))
	assert.Equal(t, false, m.completed(2))

	m.setCompleted(2)
	assert.Equal(t, true, m.completed(1))
	assert.Equal(t, true, m.completed(2))
}

func TestSequenceManager_Out_Of_Order(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(1))
	assert.Equal(t, false, m.completed(2))

	m.setCompleted(2)
	assert.Equal(t, false, m.completed(1))
	assert.Equal(t, true, m.completed(2))

	m.setCompleted(1)
	assert.Equal(t, true, m.completed(1))
	assert.Equal(t, true, m.completed(2))
}

func TestSequenceManager_Exceed_Size(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(1))
	assert.Equal(t, false, m.completed(2))
	assert.Equal(t, false, m.completed(4))

	m.setCompleted(4)
	assert.Equal(t, false, m.completed(1))
	assert.Equal(t, true, m.completed(4))
	assert.Equal(t, false, m.completed(5))

	m.setCompleted(2)
	assert.Equal(t, true, m.completed(2))

	m.setCompleted(5)
	assert.Equal(t, true, m.completed(1))
	assert.Equal(t, true, m.completed(4))
	assert.Equal(t, true, m.completed(5))
	assert.Equal(t, false, m.completed(6))
}

func TestSequenceManager_Exceed_Size_Always_Completed(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(5))

	m.setCompleted(5)
	assert.Equal(t, true, m.completed(5))
	assert.Equal(t, true, m.completed(1))
}

func TestSequenceManager_Exceed_Size_Always_Completed_With_Spacing(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(6))

	m.setCompleted(6)
	assert.Equal(t, true, m.completed(6))
	assert.Equal(t, true, m.completed(1))
}

func TestSequenceManager_Missing_Middle(t *testing.T) {
	m := newSequenceManager(4)

	m.setCompleted(1)
	m.setCompleted(2)
	m.setCompleted(3)
	m.setCompleted(4)

	m.setCompleted(6)
	m.setCompleted(8)

	assert.Equal(t, false, m.completed(7))
}

func TestSequenceManager_Missing_Middle_2(t *testing.T) {
	m := newSequenceManager(4)

	m.setCompleted(1)
	m.setCompleted(2)
	m.setCompleted(3)
	m.setCompleted(4)

	m.setCompleted(8)

	assert.Equal(t, true, m.completed(4))
	assert.Equal(t, false, m.completed(5))
	assert.Equal(t, false, m.completed(6))
	assert.Equal(t, false, m.completed(7))
	assert.Equal(t, false, m.completed(9))

	m.setCompleted(10)
	assert.Equal(t, false, m.completed(9))
}

func TestSequenceManager_Missing_Middle_3(t *testing.T) {
	m := newSequenceManager(4)

	m.setCompleted(3)

	m.setCompleted(8)

	assert.Equal(t, true, m.completed(3))
	assert.Equal(t, true, m.completed(4))
	assert.Equal(t, false, m.completed(5))
	assert.Equal(t, false, m.completed(6))
	assert.Equal(t, false, m.completed(7))
	assert.Equal(t, false, m.completed(9))

	m.setCompleted(10)
	assert.Equal(t, false, m.completed(9))
}

func TestSequenceManager_Missing_Middle_4(t *testing.T) {
	m := newSequenceManager(4)

	m.setCompleted(8)

	assert.Equal(t, true, m.completed(4))
	assert.Equal(t, false, m.completed(5))
	assert.Equal(t, false, m.completed(6))
	assert.Equal(t, false, m.completed(7))
	assert.Equal(t, false, m.completed(9))

	m.setCompleted(10)
	m.setCompleted(7)
	assert.Equal(t, false, m.completed(11))
}

func TestSequenceManager_SetAllCompleted(t *testing.T) {
	m := newSequenceManager(4)
	assert.Equal(t, false, m.completed(3))

	m.setCompleted(3)
	m.setCompleted(5)

	assert.Equal(t, false, m.completed(4))

	m.setAllCompleted(6)

	assert.Equal(t, true, m.completed(2))
	assert.Equal(t, true, m.completed(3))
	assert.Equal(t, true, m.completed(4))
	assert.Equal(t, true, m.completed(5))
	assert.Equal(t, true, m.completed(6))
	assert.Equal(t, false, m.completed(7))

	m.setCompleted(8)
	assert.Equal(t, false, m.completed(7))
	assert.Equal(t, true, m.completed(8))
}
