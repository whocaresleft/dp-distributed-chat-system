/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package topology

import (
	"fmt"
	"server/cluster/node"
)

// A Tree Manager is a component of a node that handles the links between nodes inside the Shortest Path Tree created after the leader's election.
type TreeManager struct {
	parent   *node.NodeId // Map of neighboring nodes, maps node ids to strings formatted as `<ip-address>:<port-number>`
	children map[node.NodeId]struct{}
}

// Creates a tree manager with the given parent and an empty map for children.
func NewTreeManager() *TreeManager {
	return &TreeManager{nil, make(map[node.NodeId]struct{})}
}

// Adds a child if it wasn't already present
func (t *TreeManager) AddChild(child node.NodeId) error {
	if t.ExistsChild(child) {
		return fmt.Errorf("The ID %d corresponds to an already present child", child)
	}
	t.children[child] = struct{}{}
	return nil
}

// Removes a child if present
func (t *TreeManager) Remove(child node.NodeId) error {
	if !t.ExistsChild(child) {
		return fmt.Errorf("The ID %d does not correspond to any neighbor, can't delete", child)
	}
	delete(t.children, child)
	return nil
}

// Checks whether there exists a child with the given id.
func (t *TreeManager) ExistsChild(child node.NodeId) bool {
	_, ok := t.children[child]
	return ok
}

// Returns the number of children.
func (t *TreeManager) ChildrenLength() int {
	return len(t.children)
}

// Returns true when there are no children.
// It's equivalent to `.Length() == 0`
func (t *TreeManager) IsLeaf() bool {
	return t.ChildrenLength() == 0
}

// Returns true if the node is the root of the tree.
func (t *TreeManager) IsRoot() bool {
	return t.parent == nil
}

// Sets the parent of the node
func (t *TreeManager) SetParent(parentId *node.NodeId) {
	t.parent = parentId
}

// Returns the parent of the node, or an error if it does not exist (e.g. the node is the root).
func (t *TreeManager) GetParent() (node.NodeId, error) {
	if t.IsRoot() {
		return 0, fmt.Errorf("The node is the root and does not have a parent")
	}
	return *t.parent, nil
}
