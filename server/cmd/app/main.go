package main

import (
	"fmt"
	"server/cluster"
	"server/cluster/bootstrap"
)

func wait() { var i int; fmt.Scan(&i) }

func main() {
	bootstrapNode := bootstrap.NewBootstrapNode("bootconfig.cfg")
	go bootstrapNode.StartBootstrap()

	wait()

	n1, _ := cluster.NewClusterNode(1, 46001, true)
	n1 = n1.WithDefaultContext()
	n1.BootstrapDiscovery("127.0.0.1:45999")
	go n1.Start()

	wait()

	n2, _ := cluster.NewClusterNode(2, 46002, true)
	n2 = n2.WithDefaultContext()
	n2.BootstrapDiscovery("127.0.0.1:45999")
	go n2.Start()

	wait()

}
