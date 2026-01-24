package main

import (
	"log"
	"net"
	"net/http"
	"server/cluster/nameserver"
	pb "server/cluster/nameserver/protocol"

	"google.golang.org/grpc"
)

func main() {
	ns := nameserver.NewNameServer()
	go func() {
		lis, err := net.Listen("tcp", ":45998")
		if err != nil {
			log.Fatalf("gRPC listen error %v", err)
		}
		grpcServer := grpc.NewServer()
		pb.RegisterDiscoveryServer(grpcServer, ns)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC Serve error %v", err)
		}
	}()
	http.HandleFunc("/", ns.UserEntryPoint)
	if err := http.ListenAndServe(":9999", nil); err != nil {
		log.Fatal(err)
	}
}
