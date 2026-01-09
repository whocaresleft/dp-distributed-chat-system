package main

import (
	"fmt"
	"os"
	"server/cluster"
)

func main() {
	arg := os.Args[1]

	switch arg {
	case "1":
		n1, _ := cluster.NewClusterNode(1, 46001)
		go n1.RunDispatcher()
		go n1.ElectionHandle()
	case "2":
		n2, _ := cluster.NewClusterNode(2, 46002)
		go n2.RunDispatcher()
		go n2.ElectionHandle()

		wait()
		n2.AddNeighbor(1, "127.0.0.1:46001")
	case "3":
		n2, _ := cluster.NewClusterNode(3, 46003)
		go n2.RunDispatcher()
		go n2.ElectionHandle()

		wait()
		n2.AddNeighbor(2, "127.0.0.1:46002")
	}

	wait()
	fmt.Print("Done...")
}

func wait() {
	var i int
	fmt.Scan(&i)
}
