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
	if len(os.Args) < 2 {
		fmt.Printf("Config file path needed...\nUsage: %s <.env-path>\n", os.Args[0])
		return
	}

	configPath := os.Args[1]

	if err := os.MkdirAll(configPath, 0755); err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	cfg, err := internal.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Could not load Config for node... Check '.cfg' file. {%v}\n", err)
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
