/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package control

import (
	"server/cluster/node"
	"server/cluster/topology"
	"time"
)

//===================================================//
//                                                   //
//    Control plane - Tree and post election logic   //
//                                                   //
//   It is still part of the control plane manager   //
//                                                   //
//     Different file for better readability         //
//                                                   //
//===================================================//

// becomeTreeDone is a routine that a node performs after completing his local construction of the SPT.
// That is, each neighbor, either became a tree neighbor, or answered no. Either way, concluded his part, and can now
// calculate the data plane env parameters.
func (c *ControlPlaneManager) becomeTreeDone() {

	c.treeMan.SwitchToState(topology.TreeDone)
	roleFlags := c.updateTreeRole()

	c.logfTree("No more neighbors to await, becoming DONE. My roles: %v", roleFlags.String())

	runtime := c.runtimeCtx.Load()
	runtime.SetRoles(roleFlags)
	c.updateRuntime(runtime)

	c.finishUpPostCtx()
}

// updateTreeRole calculates the node's role flags based on the local tree topology.
// The rule in this system is the following:
//   - Root (leader) => Single writer
//   - Nodes 1-hop from the Root => Readers (replicas) if they are not leaves
//   - Leaves => Input (http server, overwrites replica role)
func (c *ControlPlaneManager) updateTreeRole() node.NodeRoleFlags {
	var roleFlags node.NodeRoleFlags = node.RoleFlags_NONE

	treeParent, treeChildren, hasTreeParent := c.GetTreeNeighbors()
	leaderId := c.runtimeCtx.Load().lastElection.GetLeaderID()
	if !hasTreeParent && c.GetId() == leaderId { // I'm root
		roleFlags |= node.RoleFlags_LEADER | node.RoleFlags_PERSISTENCE

		if len(treeChildren) == 0 { // The only case where a persistence node is also an input, when there is 1 node (a single server)
			roleFlags |= node.RoleFlags_INPUT
		}
	} else {
		if len(treeChildren) == 0 { // Leaves => Input
			roleFlags |= node.RoleFlags_INPUT
		} else {
			if treeParent == leaderId { // MaxHops from leader: 1 => Storage
				roleFlags |= node.RoleFlags_PERSISTENCE
			}
		}
	}
	return roleFlags
}

// finishUpPostCtx is a routine in charge of precisely setting the fields inside the data plane env DTO.
// Also, the final steps are performed:
//   - Root: starts the tree epoch counter, forwarding to children, as well as forwaring its port (starting a cascade effect) downwards
//   - Leaves: start sending next hops messages upwards
func (c *ControlPlaneManager) finishUpPostCtx() {
	runtime := c.runtimeCtx.Load()

	if runtime.GetLastElection() == nil {
		c.logf("No election has been hold yet. Cannot finish postCtx")
		return
	}
	roleFlags := runtime.GetRoles()

	// Connections are ascending, since data comes from the input nodes, they connect to
	// their parent, who do the same, up until rool. Each node has to bind a socket, but the leaves, ho only connect
	c.dataPlaneEnv = DataPlaneEnv{
		CanWrite: false,
		CanRead:  false,
		MakeBind: !c.treeMan.IsLeaf(),
		Runtime:  &c.runtimeCtx,
	}

	if roleFlags&node.RoleFlags_LEADER > 0 {
		c.dataPlaneEnv.CanWrite = true
		c.dataPlaneEnv.CanRead = true
		go c.startTreeEpochCounter() // Start the async counter sender
		c.logfTree("Sending all of my children my port for the data plane...")
		c.forwardPortToChildren() // Send (once) the dataplane port to children

	}
	if roleFlags&node.RoleFlags_PERSISTENCE > 0 {
		c.dataPlaneEnv.CanWrite = false
		c.dataPlaneEnv.CanRead = true
	}
	if roleFlags&node.RoleFlags_INPUT > 0 { // Start input
		if parent, hasParent := c.GetTreeParent(); hasParent {
			c.logfTree("Telling my parent (%d) I am input node", parent)

			// Tells parent: "To get to <id>, send to <id>". This is because, later the parent will
			// send its parent as well the message "To get to <id>, send to <id_p>"
			// (basically each node tells its parent, to get to this input node, go through me)
			c.SendMessageTo(parent, c.newTreeInputNextHopMessage(parent, c.GetId()))
		}
	}
}

// forwardPortToChildren sends the data plane port to all the children in the SPT.
// As stated before, this is useful so that children know to what port to connect to send data to the parent
func (c *ControlPlaneManager) forwardPortToChildren() {
	children := c.GetTreeChildren()
	dataPort := c.GetDataPort()
	for child := range children {
		c.logfTree("Sending (%d) the port (%d)", child, dataPort)
		c.SendMessageTo(child, c.newTreeParentPortMessage(child, dataPort))
	}
	c.logfTree("ANYWAYS IM SETTING THIS ENV: %v", c.dataPlaneEnv)

	c.dataPlaneEnv.MakeBind = len(children) > 0
	c.sendDataEnv() // I could be ready to give the ctx to cluster
}

// startTreeEpochCounter starts a process where the ROOT of the SPT sends, at a fixed interval, an increasing counter to each child.
// Each child also forwards it until it gets to the leaves. This counter is mainly used in healing, when we are trying to figure out if other neighbors
// are still attached to the root or not (e.g. his counter is still going while mine is stuck)
func (c *ControlPlaneManager) startTreeEpochCounter() {

	c.logfTree("Started tree epoch counter. Initial value {0}")
	var counter uint64 = 0

	ticker := time.NewTicker(2 * time.Second)

	_, children, _ := c.treeMan.GetTreeNeighbors()
	for range ticker.C {
		for child := range children {
			c.SendMessageTo(child, c.newTreeEpochMessage(child, counter))
			c.logfTree("Sending current counter{%d} to %d", counter, child)
		}
		counter++
	}
}

// startShout starts the SHOUT protocol to broadcast the "LEADER" as well as constructing the SPT
// The winner (leader) starts the SHOUT by sending leader announcement messages (which act as Q for the SPT construction).
// Leader responses are used as (1) a confirmation for the leader (of the knowledge of other nodes). And as an Answer to the Q.
func (c *ControlPlaneManager) startShout() {

	c.logfTree("Setting up spanning tree process, im INITIATOR")
	c.treeMan.SetRoot(true)
	c.logfTree("Root{true}, Parent{none}, TreeNeighbors{[]}")

	electionId := c.electionCtx.GetId()
	onNeighbors := 0
	for _, neighbor := range c.NeighborList() {
		if c.IsAlive(neighbor) {
			onNeighbors++
			c.SendMessageTo(neighbor, c.newLeaderAnnouncementMessage(neighbor, c.GetId(), electionId, 0))
			c.logfElection("Sent leader message to %d", neighbor)
			c.logfTree("Sent Q message to %d", neighbor)
		} else {
			c.logfElection("NOT sent leader message to %d", neighbor)
			c.logfTree("NOT sent Q message to %d", neighbor)
		}
	}
	c.endElection(c.GetId(), electionId)

	if onNeighbors > 0 {
		c.logfTree("Awaiting responses... becoming ACTIVE")
		c.treeMan.SwitchToState(topology.TreeActive)
	} else {
		c.logfTree("I have no ON neighbors... I am alone")
		c.becomeTreeDone()
	}
}
