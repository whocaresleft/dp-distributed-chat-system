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

const folderName = "BOOTSTRAP"

func main() {
	if err := os.MkdirAll(folderName, 0755); err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bootstrap, err := bootstrap.NewBootstrapNode(
		fmt.Sprintf("%s/bootstrap.log", folderName),
		fmt.Sprintf("%s/bootstrap.cfg", folderName),
	)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}

	bootstrap.LoadConfig()
	bootstrap.StartBootstrap(ctx, 45999)

	<-ctx.Done()
	fmt.Printf("Shutting down...")
}
