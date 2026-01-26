/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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

// NameServer listen to incoming nodes and registers their IP and PORT
// It also listens to incoming HTTP connections on port 9999, just to redirect
// them onto one of the registered ones
type NameServer struct {
	pb.UnimplementedDiscoveryServer
	servers           []Addr // List of registerd (input) nodes
	mu                sync.Mutex
	roundRobinCounter int
}

// NewNameServer creates an empty nameserver
func NewNameServer() *NameServer {
	return &NameServer{
		servers:           make([]Addr, 0),
		roundRobinCounter: 0,
	}
}

// RegisterNode receives a gRPC, NodeInfo, request, adds the node amongst the other active ones and returns a response
func (s *NameServer) RegisterNode(ctx context.Context, req *pb.NodeInfo) (rep *pb.NodeInfoResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.servers = append(s.servers, Addr{req.Ip, req.Port})
	return &pb.NodeInfoResponse{Success: true}, nil
}

// UserEntryPoint is used to redirect incoming HTTP connections towards one of the registered nodes
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
