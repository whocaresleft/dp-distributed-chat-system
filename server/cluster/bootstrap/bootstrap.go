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

var neighborCountSequence = []uint8{1, 1, 1, 2, 1, 1, 1, 3, 1, 2, 1, 3, 2, 1, 3, 1, 2, 3, 1}

type partecipantNode struct {
	Id          node.NodeId `json:"id"`
	FullAddress string      `json:"full-address"`
}

// Who is the leader, which node has which address
type BootstrapNode struct {
	protocol.UnimplementedBootstrapServiceServer

	configFile string

	assigningIndex uint64
	numberIndex    uint8

	partecipants []partecipantNode
	topology     map[node.NodeId]([]node.NodeId)

	configLock   sync.Mutex
	inMemoryLock sync.RWMutex
}

func NewBootstrapNode(configFile string) *BootstrapNode {
	return &BootstrapNode{
		configFile:     configFile,
		assigningIndex: 0,
		numberIndex:    0,
		partecipants:   []partecipantNode{},
		topology:       make(map[node.NodeId]([]node.NodeId)),
		configLock:     sync.Mutex{},
		inMemoryLock:   sync.RWMutex{},
	}
}

func (b *BootstrapNode) Register(ctx context.Context, req *protocol.RegisterRequest) (*protocol.RegisterResponse, error) {
	nodeId := node.NodeId(req.Id)
	port := uint16(req.Port)

	b.inMemoryLock.Lock()

	// Retrieve, or add and then retrieve topology for node
	neighborIds := b.RegisterNode(nodeId, req.Address, port)

	neighborMap := make(map[uint64]string)
	for _, neighborId := range neighborIds {
		p, err := b.getPartecipantById(neighborId)
		if err == nil {
			neighborMap[uint64(neighborId)] = p.FullAddress
		}
	}

	b.inMemoryLock.Unlock()

	return &protocol.RegisterResponse{Success: true, Neighbors: neighborMap}, nil
}

func (b *BootstrapNode) getPartecipantById(id node.NodeId) (partecipantNode, error) {
	for _, node := range b.partecipants {
		if node.Id == id {
			return node, nil
		}
	}
	return partecipantNode{}, fmt.Errorf("The node %d is not present", id)
}

func (b *BootstrapNode) StartBootstrap() {
	lis, err := net.Listen("tcp", ":45999")
	if err != nil {
		log.Fatalf("%v", err)
	}
	grpcServer := grpc.NewServer()
	protocol.RegisterBootstrapServiceServer(grpcServer, b)

	fmt.Printf("Awaiting to serve")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("%v", err)
	}
}

func (b *BootstrapNode) RegisterNode(nodeId node.NodeId, ipAddress string, port uint16) []node.NodeId {
	port16 := uint16(port)
	fullAddr := fmt.Sprintf("%s:%d", ipAddress, port16)

	isThere := false
	for _, partecipant := range b.partecipants {
		if partecipant.Id == nodeId {
			isThere = true
			break
		}
	}
	if isThere {
		return b.topology[nodeId]
	}

	partecipantCount := len(b.partecipants)
	if partecipantCount == 0 {
		b.partecipants = append(b.partecipants, partecipantNode{nodeId, fullAddr})
		b.topology[nodeId] = []node.NodeId{}
		go b.StoreConfig()
		return []node.NodeId{}
	}

	// We need to decide its neighbors
	neighborCount := int(neighborCountSequence[b.nextNumberIndex()]) // How many neighbors?
	if neighborCount > partecipantCount {
		neighborCount = partecipantCount
	}

	neighbors := []node.NodeId{}

	for range neighborCount {
		nextId := node.NodeId(b.nextAssigningIndex())
		nextNeighbor := b.partecipants[nextId]
		neighbors = append(neighbors, nextNeighbor.Id)
	}

	b.partecipants = append(b.partecipants, partecipantNode{nodeId, fullAddr}) // Add it later to not cause inconsitencies
	b.topology[nodeId] = neighbors
	for _, id := range neighbors {
		b.topology[id] = append(b.topology[id], nodeId)
	}

	go b.StoreConfig()

	return b.topology[nodeId]
}

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

func (b *BootstrapNode) nextNumberIndex() uint8 {
	b.numberIndex++
	return b.numberIndex % uint8(len(neighborCountSequence))
}
func (b *BootstrapNode) nextAssigningIndex() uint64 {
	b.assigningIndex++
	return b.assigningIndex % uint64(len(b.partecipants))
}

func (b *BootstrapNode) StoreConfig() error {

	b.inMemoryLock.RLock()
	toSave := struct {
		AssigningIndex uint64            `json:"assigning-index"`
		NumberIndex    uint8             `json:"number-index"`
		Partecipants   []partecipantNode `json:"partecipants"`
	}{
		b.assigningIndex,
		b.numberIndex,
		b.partecipants,
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

	b.configLock.Unlock()

	return nil
}

func (b *BootstrapNode) LoadConfig() error {
	b.configLock.Lock()
	file, err := os.OpenFile(b.configFile, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	var toSavestruct struct {
		AssigningIndex uint64            `json:"assigning-index"`
		NumberIndex    uint8             `json:"number-index"`
		Partecipants   []partecipantNode `json:"partecipants"`
	}

	if err = json.Unmarshal(payload, &toSavestruct); err != nil {
		return err
	}

	b.configLock.Unlock()

	b.inMemoryLock.Lock()
	b.assigningIndex = toSavestruct.AssigningIndex
	b.numberIndex = toSavestruct.NumberIndex
	b.partecipants = toSavestruct.Partecipants
	b.inMemoryLock.Unlock()

	return nil
}
