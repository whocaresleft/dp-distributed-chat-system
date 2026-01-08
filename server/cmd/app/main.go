package main

import (
	"fmt"
	"server/cluster"
)

func main() {

	n1, _ := cluster.NewClusterNode(1, 46000)
	go n1.RunDispatcher()
	go n1.ElectionHandle()

	n2, _ := cluster.NewClusterNode(2, 46001)
	go n2.RunDispatcher()
	go n2.ElectionHandle()

	n2.AddNeighbor(1, "127.0.0.1:46000")

	var i int
	fmt.Print("Done...")
	fmt.Scan(&i)
}
