package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"server/cluster"
	"server/cluster/bootstrap"
	"syscall"
)

func wait() { var i int; fmt.Scan(&i) }

func main() {
	bootstrapNode := bootstrap.NewBootstrapNode("bootconfig.cfg")
	go bootstrapNode.StartBootstrap()

	wait()

	n1, _ := cluster.NewClusterNode(1, 46001, true)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	n1.SetContextCancelFunc(ctx, stop)
	n1.BootstrapDiscovery("127.0.0.1:45999")
	go n1.Start()

	wait()

	n2, _ := cluster.NewClusterNode(2, 46002, true)
	n2.SetContextCancelFunc(ctx, stop)
	n2.BootstrapDiscovery("127.0.0.1:45999")
	go n2.Start()

	wait()

	n3, _ := cluster.NewClusterNode(3, 46003, true)
	n3.SetContextCancelFunc(ctx, stop)
	n3.BootstrapDiscovery("127.0.0.1:45999")
	go n3.Start()

	<-ctx.Done()
	fmt.Printf("Shutting off...\n")

}
