package main

import (
	"fmt"
	"os"
	"server/cluster"
	"strconv"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		return
	}
	p, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return
	}
	switch p {
	case 1:
		n, _ := cluster.NewClusterNode(1, 46000+uint16(1), true)
		go n.RunInputDispatcher()
		go n.RunOutputDispatcher()
		go n.JoinHandle()
		go n.ElectionHandle()
		wait()
		n.AddNeighbor(2, "127.0.0.1:46002")

	case 2:
		n, _ := cluster.NewClusterNode(2, 46000+uint16(2), true)
		go n.RunInputDispatcher()
		go n.RunOutputDispatcher()
		go n.JoinHandle()
		go n.ElectionHandle()

		wait()
		n.AddNeighbor(3, "127.0.0.1:46003")
	case 3:
		n, _ := cluster.NewClusterNode(3, 46000+uint16(3), true)
		go n.RunInputDispatcher()
		go n.RunOutputDispatcher()
		go n.JoinHandle()
		go n.ElectionHandle()

		wait()
		n.AddNeighbor(1, "127.0.0.1:46001")
	case 4:
		n, _ := cluster.NewClusterNode(4, 46000+uint16(4), true)
		go n.RunInputDispatcher()
		go n.RunOutputDispatcher()
		go n.JoinHandle()
		go n.ElectionHandle()

		wait()
		n.AddNeighbor(2, "127.0.0.1:46002")
		n.AddNeighbor(3, "127.0.0.1:46003")
	}

	wait()
	fmt.Printf("Done...")
}

func wait() {
	var i int
	fmt.Scan(&i)
}
