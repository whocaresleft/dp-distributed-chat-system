package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"server/cluster"
	"server/cluster/bootstrap"
	"server/cluster/node"
	"syscall"
	"time"
)

func wait() { var i int; fmt.Scan(&i) }

func main() {
	bootstrapNode, _ := bootstrap.NewBootstrapNode("bootstrap.log", "bootconfig.cfg")
	bootstrapNode.LoadConfig()
	go bootstrapNode.StartBootstrap()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	for i := range 8 {
		time.Sleep(time.Second * 3)
		n, _ := cluster.NewClusterNode(node.NodeId(i+1), 46001+uint16(i), true)
		n.SetContextCancelFunc(ctx, stop)
		n.BootstrapDiscovery("127.0.0.1:45999")
		go n.Start()
	}

	<-ctx.Done()
	fmt.Printf("Shutting off...\n")

}
