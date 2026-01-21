/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package cluster

import (
	"context"
	"fmt"
	bootstrap_protocol "server/cluster/bootstrap/protocol"
	"server/cluster/control"
	"server/cluster/election"
	"server/cluster/network"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/topology"
	"server/internal"
	"server/internal/data"
	"server/internal/input"
	"server/internal/repository"
	"server/internal/service"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// A Cluster Node represents a single node in the distributed system. It holds different components of the node together
type ClusterNode struct {
	ready  atomic.Bool
	config *internal.Config

	ctx          context.Context
	cancel       context.CancelFunc
	logicalClock *node.LogicalClock
	logger       *nlog.NodeLogger

	controlMan  *control.ControlPlaneManager
	dataEnvChan chan control.DataPlaneEnv

	dataMan *data.DataPlaneManager

	inputMan       *input.InputManager
	persistenceMan *struct{}
}

// Creates a new cluster node with the given ID and on the given port
// It returns a pointer to said node if no problems arise. Otherwise, the pointer is nil and an appropriate error is returned
func NewClusterNode(cfg *internal.Config) (*ClusterNode, error) {

	id := node.NodeId(cfg.NodeId)
	ctrPlaneCfg, err := node.NewNodeConfig(id, int(cfg.ControlPlanePort), int(cfg.DataPlanePort))
	if err != nil {
		return nil, err
	}

	logger, err := nlog.NewNodeLogger(id, cfg.EnableLogging)
	if err != nil {
		return nil, err
	}

	clock := node.NewLogicalClock()

	mainLogger, err := logger.RegisterSubsystem("main")
	if err != nil {
		return nil, err
	}
	topologyLogger, err := logger.RegisterSubsystem("join")
	if err != nil {
		return nil, err
	}
	electionLogger, err := logger.RegisterSubsystem("election")
	if err != nil {
		return nil, err
	}
	treeLogger, err := logger.RegisterSubsystem("tree")
	if err != nil {
		return nil, err
	}
	heartbeatLogger, err := logger.RegisterSubsystem("heartbeat")
	if err != nil {
		return nil, err
	}
	dataLogger, err := logger.RegisterSubsystem("data")
	if err != nil {
		return nil, err
	}
	if _, err := logger.RegisterSubsystem("input"); err != nil {
		return nil, err
	}

	go logger.Run()
	mainLogger.Logf("Configuration component correctly created: Id{%d}, Ports{Control: %d, Data: %d}", id, cfg.ControlPlanePort, cfg.DataPlanePort)

	tp, err := topology.NewTopologyManager(id, cfg.ControlPlanePort)
	if err != nil {
		return nil, err
	}
	ec := election.NewElectionContext(id)
	tm := topology.NewTreeManager()

	dataEnvChan := make(chan control.DataPlaneEnv, 10)

	control := control.NewControlPlaneManager(ctrPlaneCfg)
	control.SetClock(clock)
	control.SetMainLogger(mainLogger)
	control.SetTopologyEnv(tp, topologyLogger)
	control.SetElectionEnv(ec, electionLogger)
	control.SetTreeEnv(tm, treeLogger)
	control.SetHeartbeatEnv(heartbeatLogger)
	control.SetDataEnvChannel(dataEnvChan)

	connMan, err := network.NewConnectionManager(id)
	if err != nil {
		return nil, err
	}

	dm := data.NewDataPlaneManager(connMan)
	dm.SetHostFinder(tp)
	dm.SetLogger(dataLogger)
	dm.SetClock(clock)

	//im := input.NewInputManager()
	//im.SetLogger(inputLogger)

	logger.Logf("main", "Created context")
	logger.Logf("main", "Node is all set")

	return &ClusterNode{
		ready:  atomic.Bool{},
		config: cfg,

		ctx:            nil,
		cancel:         nil,
		logicalClock:   clock,
		logger:         logger,
		controlMan:     control,
		dataEnvChan:    dataEnvChan,
		dataMan:        dm,
		inputMan:       nil,
		persistenceMan: nil,
	}, nil
}

func (n *ClusterNode) DefaultContext() error {
	if n.ready.Load() {
		return fmt.Errorf("A context was already set...")
	}
	n.ready.Store(true)
	n.ctx, n.cancel = context.WithCancel(context.Background())
	return nil
}

func (n *ClusterNode) SetCustomContext(ctx context.Context, cancel context.CancelFunc) error {
	if n.ready.Load() {
		return fmt.Errorf("A context was already set...")
	}
	n.ready.Store(true)
	n.ctx, n.cancel = ctx, cancel
	return nil
}

func (n *ClusterNode) EnableLogging() {
	n.logger.EnableLogging()
}

func (n *ClusterNode) DisableLogging() {
	n.logger.DisableLogging()
}

// Logs the given string. Wrap around logger.Printf
func (n *ClusterNode) logf(filename, format string, a ...any) {
	n.logger.Logf(filename, fmt.Sprintf("{%d}. %s", n.logicalClock.Snapshot(), format), a...)
}

// Increments this node's logical clock and returns its value
func (n *ClusterNode) IncrementClock() uint64 {
	return n.logicalClock.IncrementClock()
}

// Updates this node's logical clock based on the received one
func (n *ClusterNode) UpdateClock(received uint64) {
	n.logicalClock.UpdateClock(received)
}

func (n *ClusterNode) Start() error {
	if !n.ready.Load() {
		return fmt.Errorf("Node is not ready. Either the default or a custom context must be set.")
	}

	n.logf("main", "Node booting up...")
	defer n.logf("main", "Node's goroutine started correctly")

	if !n.controlMan.IsReady() {
		return fmt.Errorf("Control Plane Manager is not ready... Missing components")
	}
	go n.controlMan.Run(n.ctx)

	go n.waitForDataEnv(n.ctx)

	return nil
}

func (n *ClusterNode) BootstrapDiscovery() error {
	conn, err := grpc.NewClient(n.config.BootstrapServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := bootstrap_protocol.NewBootstrapServiceClient(conn)

	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()

	res, err := client.Register(ctx, &bootstrap_protocol.RegisterRequest{
		Id:      uint64(n.getId()),
		Address: network.GetOutboundIP(),
		Port:    uint32(n.getControlPort()),
	})
	if err != nil {
		return err
	}
	if res.Success {
		n.controlMan.ResetTree()

		addresses := make(map[node.NodeId]node.Address, 0)
		for k, v := range res.Neighbors {
			addresses[node.NodeId(k)] = node.Address{Host: v.Host, Port: uint16(v.Port)}
		}

		n.logf("main", "Neighbors recovered: %v", res.Neighbors)
		n.controlMan.RegisterNeighborBatch(addresses)
	}
	return nil
}

func (n *ClusterNode) waitForDataEnv(ctx context.Context) {

	n.logf("data", "Started awaiting for Data Plane Envirovement config...")

	for {
		select {
		case <-ctx.Done():
			n.logf("data", "Data envirovement waiter closed...")
			return

		case env := <-n.dataEnvChan:
			n.logf("data", "Received envirovement config{%v}", env)
			n.setupAndStartDataPlane(ctx, env)
		}
	}
}

func (n *ClusterNode) setupAndStartDataPlane(ctx context.Context, env control.DataPlaneEnv) {

	runtime := env.Runtime.Load()
	roleFlags := runtime.GetRoles()
	//dataplane.Stop()
	//dataplane.Reset()
	n.dataMan.SetRWPermissione(env.CanWrite, env.CanRead)
	if env.MakeBind {
		n.dataMan.BindPort(n.getDataPort())
	}
	n.dataMan.SetRuntime(env.Runtime)
	//n.dataMan.Start(ctx)

	dl, _ := n.logger.GetSubsystemLogger("data")
	var userRepo repository.UserRepository = nil
	if roleFlags&node.RoleFlags_PERSISTENCE > 0 { // Do I create the repositories?
		db, err := gorm.Open(sqlite.Open(fmt.Sprintf("NODE_%d/chat-server.sql", n.getId())), &gorm.Config{})
		if err != nil {
			n.logf("main", "FATAL: Database could not be opened correctly")
			n.cancel()
			return
		}
		userRepo = repository.NewSQLiteUserRepository(db)
	}
	authService := service.NewAuthService(userRepo, n.dataMan, dl)

	if roleFlags&node.RoleFlags_INPUT > 0 {
		n.setupInputManager(authService, ctx)
	} else {
		n.stopInputManager()
	}
}

//============================================================================//
//  InputManager                                                              //
//============================================================================//

func (n *ClusterNode) setupInputManager(authService service.AuthService, ctx context.Context) {
	if n.inputMan == nil {
		n.inputMan = input.NewInputManager()
	}
	if n.inputMan.IsRunning() {
		n.inputMan.SetPause(true)
		n.inputMan.SetAuthService(authService)
		n.inputMan.SetPause(false)
	} else {
		n.inputMan.SetAuthService(authService)
		go n.inputMan.Run(ctx, n.getInputManagerConfig())
	}
}

func (n *ClusterNode) stopInputManager() {
	if n.inputMan.IsRunning() {
		n.inputMan.Stop()
	}
}

func (n *ClusterNode) getInputManagerConfig() *input.IptConfig {
	return &input.IptConfig{
		ServerPort:        n.config.HTTPServerPort,
		ReadTimeout:       n.config.ReadTimeout,
		WriteTimeout:      n.config.WriteTimeout,
		TemplateDirectory: n.config.TemplateDirectory,
		SecretKey:         n.config.SecretKey,
	}
}

//============================================================================//
//  RepositoryManager                                                         //
//============================================================================//

//============================================================================//
//  Wrappers for NodeConfig component                                         //
//============================================================================//

// Returns the ID of the node.
func (n *ClusterNode) getId() node.NodeId {
	return node.NodeId(n.config.NodeId)
}

// Returns the port of the node used for the control plane.
func (n *ClusterNode) getControlPort() uint16 {
	return n.config.ControlPlanePort
}

// Returns the port of the node used for the data plane.
func (n *ClusterNode) getDataPort() uint16 {
	return n.config.DataPlanePort
}
