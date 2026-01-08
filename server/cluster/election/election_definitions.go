package election

import "time"

const Timeout = time.Second * 30

type LinkDirection bool

const ( // Suppose i is this node and j is the other
	Incoming LinkDirection = false // (j, i)
	Outgoing LinkDirection = true  // (i, j)
)

func (l *LinkDirection) inverse() LinkDirection {
	return !(*l)
}

////////////////////////////////////////////////

type ElectionStatus uint8

const (
	Source ElectionStatus = iota
	InternalNode
	Sink
	Lost
	Leader
)

////////////////////////////////////////////////

type ElectionState uint8

const (
	Idle ElectionState = iota
	WaitingYoDown
	WaitingYoUp
)
