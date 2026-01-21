package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"server/cluster/bootstrap"
	"syscall"
)

func wait() { var i int; fmt.Scan(&i) }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bootstrap, _ := bootstrap.NewBootstrapNode("NODE_-1/bootstrap.log", "NODE_-1/bootconfig.cfg")
	bootstrap.LoadConfig()
	bootstrap.StartBootstrap(ctx, 45999)

	<-ctx.Done()
	fmt.Printf("Shutting down...")
}
