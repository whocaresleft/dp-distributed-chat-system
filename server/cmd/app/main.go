package main

import (
	"fmt"
	"server/cluster"
)

func main() {
	n, _ := cluster.NewClusterNode(1, 46000)
	m, _ := cluster.NewClusterNode(2, 46001)

	fmt.Printf("%s", n.AddNeighbor(2, "127.0.0.1:46001"))
	fmt.Printf("%s", m.AddNeighbor(1, "127.0.0.1:46000"))
}
