package topology

import (
	"fmt"
	"server/cluster/node"
	"sync"
)

// TreeState is the state of the tree during the SPT construction
type TreeState uint8

const (
	TreeIdle   TreeState = iota // Haven't received Q yet
	TreeActive                  // Sent Q and waiting As (YES/NO)
	TreeDone                    // Received all As
)

var treeStatesNames = map[TreeState]string{
	TreeIdle:   "Idle",
	TreeActive: "Active",
	TreeDone:   "Done",
}

// String returns the readable representation of a TreeState
func (t TreeState) String() string {
	return treeStatesNames[t]
}

// Number of timeouts before considering a tree neighbor off and removing it
const TolerableTreeNeighborTimeouts = uint8(11)

// TreeManager is responsible for handling the links between tree neighbors during the SPT construction.
type TreeManager struct {
	mutex sync.RWMutex

	root          bool                  // Is this root?
	hasParent     bool                  // Has this got a parent?
	parent        node.NodeId           // Parent node ID
	state         TreeState             // State of the tree (Idle, Active, Done)
	epoch         uint64                // Current tree epoch (forwarded from the root towards the leaves)
	rootHopCount  uint64                // Number of hops from root
	treeNeighbors map[node.NodeId]bool  // Map of tree neighbors. Each node ID is a topology neighbor, the bool represents the answer to the Q message, either YES (true) or NO (false)
	timeouts      map[node.NodeId]uint8 // Traces every tree neighbor's timeouts
}

// NewTreeManager Creates and returns an empty tree manager
func NewTreeManager() *TreeManager {

	return &TreeManager{
		mutex:         sync.RWMutex{},
		root:          false,
		hasParent:     false,
		parent:        0,
		epoch:         0,
		rootHopCount:  0,
		state:         TreeIdle,
		treeNeighbors: make(map[node.NodeId]bool),
		timeouts:      make(map[node.NodeId]uint8),
	}
}

// SetRoot sets the root flag of this node to root
func (t *TreeManager) SetRoot(root bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.root = root

	if root {
		t.rootHopCount = 0
		t.hasParent = false
	}
}

// IsRoot returns true if the node is the root of the tree
func (t *TreeManager) IsRoot() bool {
	return t.root
}

// IsLeaf returns true when there are no children (node is leaf)
func (t *TreeManager) IsLeaf() bool {
	_, children, _ := t.GetTreeNeighbors()
	return len(children) == 0
}

// HasParent returns true if the node has a parent in the tree
func (t *TreeManager) HasParent() bool {
	return t.hasParent
}

// SetParent sets id as the parent in the tree, setting also the hop count from root.
// When successful, error is nil
func (t *TreeManager) SetParent(id node.NodeId, rootHopCount uint64) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.IsRoot() {
		return fmt.Errorf("Node is root, can't set parent")
	}
	t.parent = id
	t.rootHopCount = rootHopCount
	t.hasParent = true
	return nil
}

// GetParent returns the id of the parent in the tree. It also returns a boolean value
// which is true only when the parent is present
func (t *TreeManager) GetParent() (node.NodeId, bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if t.IsRoot() || !t.hasParent {
		return 0, false
	}
	return t.parent, true
}

// Removes the parent in the tree
func (t *TreeManager) RemoveParent() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.IsRoot() {
		return fmt.Errorf("Node is root")
	}
	t.hasParent = false
	t.parent = 0
	return nil
}

// SetEpoch stores epoch inside the struct
func (t *TreeManager) SetEpoch(epoch uint64) {
	t.epoch = epoch
}

// GetEpoch returns the stored epoch
func (t *TreeManager) GetEpoch() uint64 {
	return t.epoch
}

// GetRootHopCount returns the hop count of this node from root
func (t *TreeManager) GetRootHopCount() uint64 {
	return t.rootHopCount
}

// AddTreeNeighbor adds id as a tree neighbor (does not specify if children or parent).
// When successful, error is nil.
//
// This also implies that id responded with YES to Q.
func (t *TreeManager) AddTreeNeighbor(id node.NodeId) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if _, ok := t.treeNeighbors[id]; ok {
		return fmt.Errorf("Neighbor %d already present", id)
	}
	t.treeNeighbors[id] = true
	return nil
}

// RemoveTreeNeighbor removes id from the tree neighbors (does not specify if children or parent).
func (t *TreeManager) RemoveTreeNeighbor(id node.NodeId) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	delete(t.treeNeighbors, id)
}

// AcknowledgeNo marks that id has responded to Q with NO.
// When successful, error is nil.
func (t *TreeManager) AcknowledgeNo(id node.NodeId) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.treeNeighbors[id]; ok {
		return fmt.Errorf("Neighbor %d already present", id)
	}
	t.treeNeighbors[id] = false
	return nil
}

// GetTreeNeighborCount returns the number of tree neighbors (doesnt distinguish parent and children)
func (t *TreeManager) GetTreeNeighborCount() int {
	return len(t.treeNeighbors)
}

// GetTreeNeighbors returns a tuple (parent, childrem, hasParent) such that
//
// parent is the parent in the tree, however this is only valid if hasParent is true
// children is a set containing the children in the tree
func (t *TreeManager) GetTreeNeighbors() (parent node.NodeId, children map[node.NodeId]struct{}, hasParent bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	children = make(map[node.NodeId]struct{})
	if t.root {
		parent = 0
		hasParent = false

		for key, answer := range t.treeNeighbors {
			if answer {
				children[key] = struct{}{}
			}
		}
	} else {
		parent = t.parent
		hasParent = t.hasParent
		for key, answer := range t.treeNeighbors {
			if answer && key != parent {
				children[key] = struct{}{}
			}
		}
	}
	return parent, children, hasParent
}

// GetChildren returns only the children in the tree
func (t *TreeManager) GetChildren() map[node.NodeId]struct{} {
	_, children, _ := t.GetTreeNeighbors()
	return children
}

// HasAnswered tell whether id has responded to Q
func (t *TreeManager) HasAnswered(id node.NodeId) bool {
	_, ok := t.treeNeighbors[id]
	return ok
}

// GetState returns the current tree state
func (t *TreeManager) GetState() TreeState {
	return t.state
}

// SwitchToState makes the manager transition to the given state
func (t *TreeManager) SwitchToState(state TreeState) {
	t.state = state
}

// IncreaseTimeout increases by 1 the timeouts of the given neighbor
func (t *TreeManager) IncreaseTimeout(treeNeighbor node.NodeId) uint8 {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if _, ok := t.timeouts[treeNeighbor]; !ok {
		t.timeouts[treeNeighbor] = 0
	}
	t.timeouts[treeNeighbor]++
	return t.timeouts[treeNeighbor]
}

// GetTimeout returns the timeouts of the given tree neighbor
func (t *TreeManager) GetTimeout(treeNeighbor node.NodeId) uint8 {
	return t.timeouts[treeNeighbor]
}

// Reset brings the manager back to its creation, emptying its content
func (t *TreeManager) Reset() {
	t.root = false
	t.parent = 0
	t.epoch = 0
	t.state = TreeIdle
	clear(t.treeNeighbors)
	clear(t.timeouts)
}
