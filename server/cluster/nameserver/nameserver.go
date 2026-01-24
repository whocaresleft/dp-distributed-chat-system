package nameserver

import (
	"context"
	"fmt"
	"net/http"
	pb "server/cluster/nameserver/protocol"
	"sync"
)

type Addr struct {
	Host, Port string
}

type NameServer struct {
	pb.UnimplementedDiscoveryServer
	servers           []Addr
	mu                sync.Mutex
	roundRobinCounter int
}

func NewNameServer() *NameServer {
	return &NameServer{
		servers:           make([]Addr, 0),
		roundRobinCounter: 0,
	}
}

func (s *NameServer) RegisterNode(ctx context.Context, req *pb.NodeInfo) (rep *pb.NodeInfoResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.servers = append(s.servers, Addr{req.Ip, req.Port})
	return &pb.NodeInfoResponse{Success: true}, nil
}

func (s *NameServer) UserEntryPoint(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.servers) == 0 {
		http.Error(w, "No chat server is avaiable now.", http.StatusServiceUnavailable)
		return
	}
	index := s.roundRobinCounter % len(s.servers)
	s.roundRobinCounter++

	server := s.servers[index]

	targetURL := fmt.Sprintf("http://%s:%s", server.Host, server.Port)
	http.Redirect(w, r, targetURL, http.StatusTemporaryRedirect)
}
