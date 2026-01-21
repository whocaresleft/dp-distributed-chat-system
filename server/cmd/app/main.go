package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"server/cluster"
	"server/internal"
	"syscall"
	"time"
)

func wait() { var i int; fmt.Scan(&i) }

func main() {

	cfg, err := internal.LoadConfig()
	if err != nil {
		fmt.Printf("Could not load Config for node... Check '.env' file. {%v}", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	n, err := cluster.NewClusterNode(cfg)
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
	}
	n.SetCustomContext(ctx, stop)
	n.BootstrapDiscovery()

	go func() {
		time.Sleep(4 * time.Second)
		go n.Start()
	}()

	<-ctx.Done()
	fmt.Printf("Shutting off...\n")

}
