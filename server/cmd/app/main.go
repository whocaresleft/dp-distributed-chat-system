package main

import (
	"fmt"
	"server/cluster"
	"server/cluster/node"
)

func main() {

	ids := []node.NodeId{2, 3}
	nodes := make(map[node.NodeId]*cluster.ClusterNode)

	for _, id := range ids {
		n, _ := cluster.NewClusterNode(id, 46000+uint16(id))
		nodes[id] = n
	}

	for _, node := range nodes {
		go node.RunInputDispatcher()
		go node.RunOutputDispatcher()
		go node.JoinHandle()
		go node.ElectionHandle()
	}

	wait()
	// Build the network

	nodes[2].AddNeighbor(3, "127.0.0.1:46003")

	wait()
	fmt.Print("Done...")
}

func wait() {
	var i int
	fmt.Scan(&i)
}
