/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"server/cluster/bootstrap/protocol"
	"server/cluster/node"
	"sync"

	"google.golang.org/grpc"
)

// Generic number sequence for the number of neighbors of a node.
var neighborCountSequence = []uint8{1, 1, 1, 2, 1, 1, 1, 3, 1, 2, 1, 3, 2, 1, 3, 1, 2, 3, 1}

// partecipantNode represents a node instance on the bootstrap server.
// partecipantNode is identified by (Id, Host, Port) Where port is the one used on the control plane of the node
type partecipantNode struct {
	Id   node.NodeId `json:"id"`   // Numeric identifier of the node
	Host string      `json:"host"` // String that is either an IP address or a hostname
	Port uint32      `json:"port"` // Port used on the control plane of the node
}

// BootstrapNode is a server whose josb is to listen to entering nodes, and assign them neighbors.
// It handles the number of neighbors, and which neighbors, using a mix of increasing (++) and decreasing (--) Round Robin.
type BootstrapNode struct {
	protocol.UnimplementedBootstrapServiceServer

	logger   *log.Logger
	fileLock sync.Mutex

	numberIndex    uint8  // This is the index used to access the previous number sequence, to retrieve the number of neighbors
	assigningIndex uint64 // This is the index used to access the current List of nodes, to retrieve the actual neighbor

	partecipants []partecipantNode               // List of connected nodes
	topology     map[node.NodeId]([]node.NodeId) // Map of the topology, each Node (marked by ID) as a list of Neighbors (node ID's)

	configFile   string // Path of the configuration file
	configLock   sync.Mutex
	inMemoryLock sync.RWMutex

	logChan chan string // Strings aren't directly written on file, but rather buffered here, and an async function writes them.
}

// NewBootstrapNode creates and returns a pointer to a BootstrapNode, configured with the given logfile and configFile.
// If the creation is successful, the error is nil, otherwise the pointer is nil.
func NewBootstrapNode(logfile string, configFile string) (*BootstrapNode, error) {
	logFile, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return nil, err
	}
	logger := log.New(logFile, "Bootstrap", log.Ldate|log.Ltime|log.Lshortfile)
	logger.Printf("Bootstrap node created, config{%s}", configFile)

	return &BootstrapNode{
		logger:         logger,
		fileLock:       sync.Mutex{},
		assigningIndex: 0,
		numberIndex:    0,
		partecipants:   []partecipantNode{},
		topology:       make(map[node.NodeId]([]node.NodeId)),
		configFile:     configFile,
		configLock:     sync.Mutex{},
		inMemoryLock:   sync.RWMutex{},

		logChan: make(chan string, 500),
	}, nil
}

// writeToLogAsync is a function that continuosly listens on a channel, and when it receives a message
// it writes it onto the logger.
// ctx is a context used to correctly stop the function in case the caller is stopped.
func (b *BootstrapNode) writeToLogAsync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case toWrite := <-b.logChan:
			b.fileLock.Lock()
			b.logger.Print(toWrite)
			b.fileLock.Unlock()
		}
	}
}

// logf composes the given format string and appends it to the channel, waiting to be logged
func (b *BootstrapNode) logf(format string, v ...any) {
	b.logChan <- fmt.Sprintf(format, v...)
}

// Register handles a gRPC request, given by a node that is asking to join the topology handled by this bootstrap.
// The bootstrap calculates his neighbors and returns them as tuple (id, host, port) insdie the response.
// When the operation is successfull, err == nil, otherwise rep == nil
func (b *BootstrapNode) Register(ctx context.Context, req *protocol.RegisterRequest) (rep *protocol.RegisterResponse, err error) {
	nodeId := node.NodeId(req.Id)
	port := uint16(req.Port)

	b.inMemoryLock.Lock()

	// Retrieve, or add and then retrieve topology for node
	neighborIds := b.RegisterNode(nodeId, req.Address, port)
	b.logf("Registered new node{%d, %s:%d}. His neighbors are {%v}", nodeId, req.Address, port, neighborIds)

	neighborMap := make(map[uint64]*protocol.NodeInfo)
	for _, neighborId := range neighborIds {
		p, err := b.getPartecipantById(neighborId)
		if err == nil {
			neighborMap[uint64(neighborId)] = &protocol.NodeInfo{Host: p.Host, Port: p.Port}
		}
	}

	b.inMemoryLock.Unlock()

	return &protocol.RegisterResponse{Success: true, Neighbors: neighborMap}, nil
}

// getPartecipantById retrieves the tuple (Id, Host, Port) of partecipantNode p, by its id.
// Returns such tuple and, if successful, err == nil, otherwise the tuple is empty and err contains an error.
func (b *BootstrapNode) getPartecipantById(id node.NodeId) (p partecipantNode, err error) {
	for _, node := range b.partecipants {
		if node.Id == id {
			return node, nil
		}
	}
	return partecipantNode{}, fmt.Errorf("The node %d is not present", id)
}

// StartBoostrap starts the gRPC server of the BootstrapNode, waiting for incoming nodes to serve.
// ctx is a context used to gracefully terminate the server in case of a sender shutoff.
// The gRPC server listens on tcp:port.
func (b *BootstrapNode) StartBootstrap(ctx context.Context, port uint16) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("%v", err)
	}
	go b.writeToLogAsync(ctx)

	b.logf("Started listening on TCP port %d", port)
	grpcServer := grpc.NewServer()
	protocol.RegisterBootstrapServiceServer(grpcServer, b)

	go func() {
		<-ctx.Done()
		b.logf("Shutting down gRPC server...")
		grpcServer.GracefulStop()
		b.logf("Server shut down. Bye bye")
	}()

	b.logf("Awaiting to serve")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("%v", err)
	}
}

// RegisterNode is the actual logic called inside the gRPC registration request handler.
// (nodeId, host, port) is the tuple the bootstrap needs to register.
// neighbors is a list of ID's, marking the neighbors of the asking node. If a node happens to get '[]' as neighbors, it means
// it is the first one in the topology.
func (b *BootstrapNode) RegisterNode(nodeId node.NodeId, host string, port uint16) (neighbors []node.NodeId) {
	b.logf("Registering new node...ID{%d}, ADDR{%s:%d}", nodeId, host, port)

	isThere := false
	for _, partecipant := range b.partecipants {
		if partecipant.Id == nodeId {
			isThere = true
			break
		}
	}
	if isThere {
		b.logf("Node %d already present", nodeId)
		return b.topology[nodeId]
	}

	b.logf("Node %d is new, calculating his neighbors", nodeId)
	partecipantCount := len(b.partecipants)
	if partecipantCount == 0 {
		b.logf("There are no partecipants, node %d is alone", nodeId)
		b.partecipants = append(b.partecipants, partecipantNode{nodeId, host, uint32(port)})
		b.topology[nodeId] = []node.NodeId{}
		go b.StoreConfig()
		return []node.NodeId{}
	}

	// We need to decide its neighbors
	nextIdx := b.nextNumberIndex()
	neighborCount := int(neighborCountSequence[nextIdx]) // How many neighbors?
	b.logf("Calculating number of neighbors for %d: Using idx{%d}, got {%d}", nodeId, nextIdx, neighborCount)
	if neighborCount > partecipantCount {
		b.logf("[%d] There are only %d partecipants, shrinking %d => %d", nodeId, partecipantCount, neighborCount, partecipantCount)
		neighborCount = partecipantCount
	}

	neighbors = []node.NodeId{}

	for range neighborCount {
		nextId := node.NodeId(b.nextAssigningIndex())
		nextNeighbor := b.partecipants[nextId]
		neighbors = append(neighbors, nextNeighbor.Id)
		b.logf("[%d] Calculating next neighbor: Index{%d}, Neighbor{%v}, currentList{%v}", nodeId, nextId, nextNeighbor, neighbors)
	}

	b.partecipants = append(b.partecipants, partecipantNode{nodeId, host, uint32(port)}) // Add it later to not cause inconsitencies
	b.topology[nodeId] = neighbors
	for _, id := range neighbors {
		b.topology[id] = append(b.topology[id], nodeId)
	}

	go b.StoreConfig()

	return b.topology[nodeId]
}

// RemoveNode removes a node from the topology, based on the given id
// It returns nil when successful
func (b *BootstrapNode) RemoveNode(id node.NodeId) error {
	b.inMemoryLock.Lock()
	defer b.inMemoryLock.Unlock()

	i := 0
	for idx, partecipant := range b.partecipants {
		if partecipant.Id != id {
			b.partecipants[i] = b.partecipants[idx]
			i++
		}
	}
	b.partecipants = b.partecipants[:i]
	return nil
}

// nextNumberIndex calculate the next neighbor number index in the sequence.
// It uses Round Robin.
func (b *BootstrapNode) nextNumberIndex() uint8 {
	b.numberIndex++
	return b.numberIndex % uint8(len(neighborCountSequence))
}

// nextAssigningIndex calculate the mext index in the current neighbor list.
// It uses a reversed Round Robin (decreasing, circular).
func (b *BootstrapNode) nextAssigningIndex() uint64 {

	if len(b.partecipants) == 0 {
		return 0
	}

	if b.assigningIndex == 0 {
		b.assigningIndex = uint64(len(b.partecipants))
	}
	b.assigningIndex--
	return b.assigningIndex % uint64(len(b.partecipants))
}

// StoreConfig saves the current configuration of the BootstrapNode on its config file
// Everything is saved in JSON format for simplicity, and can be hand modified.
// It returns nil when succesful
func (b *BootstrapNode) StoreConfig() error {

	b.inMemoryLock.RLock()
	toSave := struct {
		AssigningIndex uint64                        `json:"assigning-index"`
		NumberIndex    uint8                         `json:"number-index"`
		Partecipants   []partecipantNode             `json:"partecipants"`
		Topology       map[node.NodeId][]node.NodeId `json:"topology"`
	}{
		b.assigningIndex,
		b.numberIndex,
		b.partecipants,
		b.topology,
	}
	b.inMemoryLock.RUnlock()

	payload, err := json.Marshal(toSave)
	if err != nil {
		return err
	}

	b.configLock.Lock()

	file, err := os.OpenFile(fmt.Sprintf("%s.tmp", b.configFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}

	fmt.Fprintf(file, "%s", payload)
	file.Close()

	os.Rename(fmt.Sprintf("%s.tmp", b.configFile), b.configFile)
	b.logf("Storing information on config file... %v", toSave)

	b.configLock.Unlock()

	return nil
}

// LoadConfig retrieves the configuration of the BootstrapNode from its config file
// Everything is saved in JSON format for simplicity, and can be hand modified.
// It returns nil when succesful
func (b *BootstrapNode) LoadConfig() error {
	b.configLock.Lock()
	file, err := os.OpenFile(b.configFile, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	b.configLock.Unlock()

	if len(payload) == 0 {
		b.StoreConfig()
	}

	var toSavestruct struct {
		AssigningIndex uint64                        `json:"assigning-index"`
		NumberIndex    uint8                         `json:"number-index"`
		Partecipants   []partecipantNode             `json:"partecipants"`
		Topology       map[node.NodeId][]node.NodeId `json:"topology"`
	}

	if err = json.Unmarshal(payload, &toSavestruct); err != nil {
		return err
	}

	b.inMemoryLock.Lock()
	b.assigningIndex = toSavestruct.AssigningIndex
	b.numberIndex = toSavestruct.NumberIndex
	b.partecipants = toSavestruct.Partecipants
	b.topology = toSavestruct.Topology
	b.inMemoryLock.Unlock()

	b.logf("Loading config from file...")
	return nil
}
