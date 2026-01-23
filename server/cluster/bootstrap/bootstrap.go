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
	Id   node.NodeId `json:"id"`
	Host string      `json:"host"`
	Port uint32      `json:"port"`
}

// Who is the leader, which node has which address
type BootstrapNode struct {
	protocol.UnimplementedBootstrapServiceServer

	logger   *log.Logger
	fileLock sync.Mutex

	assigningIndex uint64
	numberIndex    uint8

	partecipants []partecipantNode
	topology     map[node.NodeId]([]node.NodeId)

	configFile   string
	configLock   sync.Mutex
	inMemoryLock sync.RWMutex

	logChan chan string
}

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

func (b *BootstrapNode) logf(format string, v ...any) {
	b.logChan <- fmt.Sprintf(format, v...)
}

func (b *BootstrapNode) Register(ctx context.Context, req *protocol.RegisterRequest) (*protocol.RegisterResponse, error) {
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

func (b *BootstrapNode) getPartecipantById(id node.NodeId) (partecipantNode, error) {
	for _, node := range b.partecipants {
		if node.Id == id {
			return node, nil
		}
	}
	return partecipantNode{}, fmt.Errorf("The node %d is not present", id)
}

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

func (b *BootstrapNode) RegisterNode(nodeId node.NodeId, host string, port uint16) []node.NodeId {
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

	neighbors := []node.NodeId{}

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

	if len(b.partecipants) == 0 {
		return 0
	}

	if b.assigningIndex == 0 {
		b.assigningIndex = uint64(len(b.partecipants))
	}
	b.assigningIndex--
	return b.assigningIndex % uint64(len(b.partecipants))
}

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
