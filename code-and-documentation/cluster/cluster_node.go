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
	"encoding/json"
	"fmt"
	bootstrap_protocol "server/cluster/bootstrap/protocol"
	"server/cluster/control"
	"server/cluster/election"
	nameserver_protocol "server/cluster/nameserver/protocol"
	"server/cluster/network"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"server/internal"
	"server/internal/data"
	"server/internal/entity"
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

// Cluster Node represents a single node in the distributed system. It holds different components of the node together
// It manages both the control plane and data plane, as well as the presentation layer for the HTTP servers
type ClusterNode struct {
	ready  atomic.Bool      // Is node ready?
	config *internal.Config // Config struct

	ctx          context.Context    // Context
	cancel       context.CancelFunc // Cancel functiony
	logicalClock *node.LogicalClock // Logical clock
	logger       *nlog.NodeLogger   // Logger component

	controlMan  *control.ControlPlaneManager // Control plane manager
	dataEnvChan chan control.DataPlaneEnv    // Data plane envirovement channel, injected into the control plane manager

	dataMan *data.DataPlaneManager // Data plane manager

	inputMan   *input.InputManager  // Input manager
	storageMan *data.StorageManager // Storage manager
}

// NewClusterNode creates a new cluster node with the given ID and on the given port
// It returns a pointer to said node if no problems arise. Otherwise, the pointer is nil and an appropriate error is returned
func NewClusterNode(cfg *internal.Config) (*ClusterNode, error) {

	id := node.NodeId(cfg.NodeId)
	ctrPlaneCfg, err := node.NewNodeConfig(id, int(cfg.ControlPlanePort), int(cfg.DataPlanePort))
	if err != nil {
		return nil, err
	}

	clock := node.NewLogicalClock()

	logger, err := nlog.NewNodeLogger(id, cfg.EnableLogging, clock)
	if err != nil {
		return nil, err
	}

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

	mainLogger.Logf("Configuration component correctly created: Id{%d}, Ports{Control: %d, Data: %d}", id, cfg.ControlPlanePort, cfg.DataPlanePort)

	topologyConnMan, err := network.NewZeroMQConnectionManager(id)
	if err != nil {
		return nil, err
	}

	tp, err := topology.NewTopologyManager(topologyConnMan, cfg.ControlPlanePort)
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

	dataConnMan, err := network.NewZeroMQConnectionManager(id)
	if err != nil {
		return nil, err
	}

	dm := data.NewDataPlaneManager(dataConnMan)
	dm.SetHostFinder(tp)
	dm.SetLogger(dataLogger)
	dm.SetClock(clock)

	logger.Logf("main", "Created context")
	logger.Logf("main", "Node is all set")

	return &ClusterNode{
		ready:  atomic.Bool{},
		config: cfg,

		ctx:          nil,
		cancel:       nil,
		logicalClock: clock,
		logger:       logger,
		controlMan:   control,
		dataEnvChan:  dataEnvChan,
		dataMan:      dm,
		inputMan:     nil,
		storageMan:   nil,
	}, nil
}

// DefaultContext sets a default context.
// If successful, error is nil
func (n *ClusterNode) DefaultContext() error {
	if n.ready.Load() {
		return fmt.Errorf("A context was already set...")
	}
	n.ready.Store(true)
	n.ctx, n.cancel = context.WithCancel(context.Background())
	return nil
}

// SetCustomContext injects a custom context with a cancel function.
// If successful, error is nil
func (n *ClusterNode) SetCustomContext(ctx context.Context, cancel context.CancelFunc) error {
	if n.ready.Load() {
		return fmt.Errorf("A context was already set...")
	}
	n.ready.Store(true)
	n.ctx, n.cancel = ctx, cancel
	return nil
}

// EnableLogging enables the logger, making it so it writes to files again
func (n *ClusterNode) EnableLogging() {
	n.logger.EnableLogging()
}

// DisableLogging disables the logger, making it so it doesn't write to files
func (n *ClusterNode) DisableLogging() {
	n.logger.DisableLogging()
}

// logf logs the given string. Wrap around logger.Printf
func (n *ClusterNode) logf(filename, format string, a ...any) {
	n.logger.Logf(filename, format, a...)
}

// IncrementClock increments this node's logical clock and returns its value
func (n *ClusterNode) IncrementClock() uint64 {
	return n.logicalClock.IncrementClock()
}

// UpdateClock updates this node's logical clock based on the received one
func (n *ClusterNode) UpdateClock(received uint64) {
	n.logicalClock.UpdateClock(received)
}

// Start is used to start every necessary component of the Cluster Node
// If a config is not loaded or the node is not ready, an error is returned.
// Otherwise, error is nil and the following are started:
//   - Logger
//   - Contron Plane Manager
//   - Data Plane waiter (that then starts it later)
func (n *ClusterNode) Start() error {
	if !n.ready.Load() {
		return fmt.Errorf("Node is not ready. Either the default or a custom context must be set.")
	}

	n.logf("main", "Node booting up...")
	defer n.logf("main", "Node's goroutine started correctly")

	go n.logger.Run(n.ctx)

	if !n.controlMan.IsReady() {
		return fmt.Errorf("Control Plane Manager is not ready... Missing components")
	}
	go n.controlMan.Run(n.ctx)

	go n.waitForDataEnv(n.ctx)

	return nil
}

// BootstrapDiscovery connects to a bootstrap server that tells the node who are his neighbors, and to what IP and PORT to connect to talk to them
// on the control plane. If the gRPC to the bootstrap goes fine, a map of nodeId=>Address is retrieved, and is stored in the control plane so that it can
// add then to the topology once the manager is ready. Also, if successful, error is nil
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
		Id:      uint64(n.GetId()),
		Address: network.GetOutboundIP(),
		Port:    uint32(n.GetControlPort()),
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

// NameServerRegister connects to a namerserver grpc to which this node registers itself (its ip and http port)
func (n *ClusterNode) NameServerRegister() error {
	n.logf("main", "Registering HTTP server %s:%s on the nameserver", network.GetOutboundIP(), n.config.HTTPServerPort)
	conn, err := grpc.NewClient(n.config.NameserverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := nameserver_protocol.NewDiscoveryClient(conn)

	req := &nameserver_protocol.NodeInfo{
		Ip:   network.GetOutboundIP(),
		Port: n.config.HTTPServerPort,
	}

	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()

	res, err := client.RegisterNode(ctx, req)
	if err != nil {
		return err
	}
	if res.Success {
		return nil
	}
	return fmt.Errorf("Big oof")
}

// waitForDataEnv waits on a channel for a data env DTO by the control plane.
// Once received, it will setup and start the data plane, alongside the other managers
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

// setupAndStartDataPlane uses the data plane env given by the control plane manager to correctly
// set up the data plan and start it, alongside the input and storage managers, if necessary
func (n *ClusterNode) setupAndStartDataPlane(ctx context.Context, env control.DataPlaneEnv) {

	runtime := env.Runtime.Load()
	roleFlags := runtime.GetRoles()
	n.dataMan.Stop()
	n.dataMan.SetRWPermissions(env.CanRead, env.CanWrite) // Update RW
	if env.MakeBind {
		dp := n.GetDataPort()
		n.dataMan.BindPort(dp)
		n.logf("data", "Binding port %d", dp)
	}
	n.dataMan.SetRuntime(env.Runtime)

	dl, _ := n.logger.GetSubsystemLogger("data")
	if roleFlags&node.RoleFlags_PERSISTENCE > 0 {
		n.setupStorageManager()
		n.dataMan.SetEpochCacher(n.storageMan)

		// Retrieve repos, to give to service
		globalRepo := n.storageMan.GetGlobalRepository()
		userRepo := n.storageMan.GetUserRepository()
		messageRepo := n.storageMan.GetMessageRepository()
		groupRepo := n.storageMan.GetGroupRepository()

		// Setup services with local implementation
		authService := service.NewLocalAuthService(env.CanWrite, userRepo, globalRepo, dl)
		userService := service.NewLocalUserService(env.CanWrite, userRepo, globalRepo, dl)
		messageService := service.NewLocalMessageService(env.CanWrite, messageRepo, globalRepo, dl)
		groupService := service.NewLocalGroupService(env.CanWrite, groupRepo, userRepo, globalRepo, dl)

		if env.CanWrite {
			n.RegisterWriterCallbacks(authService, userService, messageService, groupService)
		} else if env.CanRead {
			n.RegisterReaderCallbacks(authService, userService, messageService, groupService)
			n.dataMan.SendWarmupRequest() // We could be out of sync with writer
		}
	}
	if roleFlags&node.RoleFlags_INPUT > 0 {
		time.Sleep(10 * time.Second) // Empirical, needs time

		// Setup services with proxy implementation
		authService := service.NewProxyAuthService(n.dataMan, dl)
		userService := service.NewProxyUserService(n.dataMan, dl)
		messageService := service.NewProxyMessageService(n.dataMan, dl)
		groupService := service.NewproxyGroupService(n.dataMan, dl)

		// Setup and start the input manager
		n.setupInputManager(authService, userService, messageService, groupService, ctx)
	} else {
		n.logf("input", "I have to stop the HTTP server, no more input node.")
		n.stopInputManager()
	}

	go n.dataMan.Run(ctx)
}

//============================================================================//
//  InputManager                                                              //
//============================================================================//

// setupInputManager sets the input manager up
func (n *ClusterNode) setupInputManager(authService service.AuthService, userService service.UserService, messageService service.MessageService, groupService service.GroupService, ctx context.Context) {

	if n.inputMan == nil {
		inputLogger, _ := n.logger.GetSubsystemLogger("input")
		n.inputMan = input.NewInputManager()
		n.inputMan.SetLogger(inputLogger)

		n.NameServerRegister()
	}
	if n.inputMan.IsRunning() {
		n.inputMan.SetPause(true)
		n.inputMan.SetAuthService(authService)
		n.inputMan.SetUserService(userService)
		n.inputMan.SetMessageService(messageService)
		n.inputMan.SetGroupService(groupService)
		n.inputMan.SetPause(false)
	} else {
		n.inputMan.SetAuthService(authService)
		n.inputMan.SetUserService(userService)
		n.inputMan.SetMessageService(messageService)
		n.inputMan.SetGroupService(groupService)
		go n.inputMan.Run(ctx, n.getInputManagerConfig())
	}
}

// stopInputManager stops the input manager
func (n *ClusterNode) stopInputManager() {
	n.logf("input", "do i stop? %v", n.inputMan != nil && n.inputMan.IsRunning())
	if n.inputMan != nil && n.inputMan.IsRunning() {
		n.logf("input", "calling stop")
		n.inputMan.Stop()
	}
}

// getInputManagerConfig returns a struct with input manager configuration
func (n *ClusterNode) getInputManagerConfig() *input.IptConfig {
	return &input.IptConfig{
		ServerPort:        n.config.HTTPServerPort,
		ReadTimeout:       n.config.ReadTimeout,
		WriteTimeout:      n.config.WriteTimeout,
		TemplateDirectory: n.config.TemplateDirectory,
		SecretKey:         n.config.SecretKey,
	}
}

// RegisterWriterCallbacks registers functions that handle the forwarder requests, by redirecting them to the appropriate service
func (n *ClusterNode) RegisterWriterCallbacks(as service.AuthService, us service.UserService, ms service.MessageService, gs service.GroupService) {
	n.logf("main", "Registering callback for %d", protocol.ActionUserRegister)
	n.dataMan.RegisterRequestHandler(protocol.ActionUserRegister, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var credentials map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &credentials); err != nil {
			return nil, err
		}

		user, okU := credentials["username"]
		tag, okT := credentials["tag"]
		passw, okH := credentials["password"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okU && okT && okH) {
			return templResponse, nil
		}

		u, newEpoch, err := as.Register(user, tag, passw)
		if err != nil {
			templResponse.ErrorMessage = "User with same credentials already exists"
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch

		templResponse.Payload, _ = json.Marshal(u)

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionUserLogin)
	n.dataMan.RegisterRequestHandler(protocol.ActionUserLogin, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var credentials map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &credentials); err != nil {
			return nil, err
		}

		user, okU := credentials["username"]
		tag, okT := credentials["tag"]
		passw, okH := credentials["password"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okU && okT && okH) {
			return templResponse, nil
		}

		u, newEpoch, err := as.Login(user, tag, passw, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionUserGetNameTag)
	n.dataMan.RegisterRequestHandler(protocol.ActionUserGetNameTag, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var searchTags map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &searchTags); err != nil {
			return nil, err
		}

		user, okU := searchTags["username"]
		tag, okT := searchTags["tag"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okU && okT) {
			return templResponse, nil
		}

		u, newEpoch, err := us.GetUserByNameTag(user, tag, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionUsersGetName)
	n.dataMan.RegisterRequestHandler(protocol.ActionUsersGetName, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {

		username := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		u, newEpoch, err := us.GetUsersByName(username, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionUserDelete)
	n.dataMan.RegisterRequestHandler(protocol.ActionUserDelete, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {

		uuid := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		newEpoch, err := us.DeleteUser(uuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "User with same credentials already exists"
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch

		templResponse.Payload = []byte(uuid)

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionMessageSend)
	n.dataMan.RegisterRequestHandler(protocol.ActionMessageSend, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var params map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &params); err != nil {
			return nil, err
		}

		sender, okS := params["sender"]
		receiver, okR := params["receiver"]
		content, okC := params["content"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okS && okR && okC) {
			return templResponse, nil
		}

		sentMsg, newEpoch, err := ms.CreateDMMessage(sender, receiver, content, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "User with same credentials already exists"
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch

		templResponse.Payload, _ = json.Marshal(sentMsg)

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMessageSend)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMessageSend, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var params map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &params); err != nil {
			return nil, err
		}

		sender, okS := params["sender"]
		group, okR := params["receiver"]
		content, okC := params["content"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okS && okR && okC) {
			return templResponse, nil
		}

		sentMsg, newEpoch, err := ms.CreateGroupMessage(sender, group, content, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "User with same credentials already exists"
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch

		templResponse.Payload, _ = json.Marshal(sentMsg)

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionMessageRecv)
	n.dataMan.RegisterRequestHandler(protocol.ActionMessageRecv, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var params map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &params); err != nil {
			return nil, err
		}

		sender, okS := params["sender"]
		receiver, okR := params["receiver"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okS && okR) {
			return templResponse, nil
		}

		msgs, newEpoch, err := ms.GetDM(sender, receiver, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(msgs)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMessageRecv)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMessageRecv, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {

		group := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		msgs, newEpoch, err := ms.GetGroup(group, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(msgs)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupCreate)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupCreate, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {

		var params struct {
			GroupName string       `json:"group-name"`
			Creator   *entity.User `json:"creator"`
		}

		err := json.Unmarshal(msg.Payload, &params)
		if err != nil {
			return nil, err
		}

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		createdGroup, newEpoch, err := gs.CreateGroup(params.GroupName, params.Creator, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = err.Error()
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch

		templResponse.Payload, _ = json.Marshal(createdGroup)

		return templResponse, nil
	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupDelete)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupDelete, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		groupUuid := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		newEpoch, err := gs.DeleteGroup(groupUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "User with same credentials already exists"
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch
		copy(templResponse.Payload, msg.Payload)

		return templResponse, nil
	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMemberJoin)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMemberJoin, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var params map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &params); err != nil {
			return nil, err
		}

		groupUuid, okG := params["group-uuid"]
		userUuid, okU := params["user-uuid"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okG && okU) {
			return templResponse, nil
		}

		newEpoch, err := gs.AddGroupUser(groupUuid, userUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		templResponse.Epoch = newEpoch
		copy(templResponse.Payload, msg.Payload)

		return templResponse, nil
	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMemberRemove)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMemberRemove, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var params map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &params); err != nil {
			return nil, err
		}

		groupUuid, okG := params["group-uuid"]
		userUuid, okU := params["user-uuid"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okG && okU) {
			return templResponse, nil
		}

		newEpoch, err := gs.RemoveGroupUser(groupUuid, userUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""
		copy(templResponse.Payload, msg.Payload)
		templResponse.Epoch = newEpoch

		return templResponse, nil
	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMembersGet)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMembersGet, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		groupUuid := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		members, newEpoch, err := gs.GetGroupMembers(groupUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, _ := json.Marshal(members)
		templResponse.Payload = payload

		templResponse.Epoch = newEpoch

		return templResponse, nil
	})

	n.logf("main", "Registering callback for %d", protocol.ActionGroupGetUUID)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupGetUUID, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		groupUuid := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		group, newEpoch, err := gs.GetGroupByUUID(groupUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(group)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil
	})

	n.logf("main", "Registering snapshot callback")
	n.dataMan.SetSnapshotCallback(func(req *protocol.DataMessage) (rep *protocol.DataMessage, err error) {
		originalSender := req.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			req.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			req.Action,
			"",
			[]byte{},
			req.Epoch,
		)

		users, err := n.storageMan.GetUserRepository().GetAll()
		if err != nil {
			templResponse.ErrorMessage = err.Error()
			return templResponse, nil
		}
		messages, err := n.storageMan.GetMessageRepository().GetAll()
		if err != nil {
			templResponse.ErrorMessage = err.Error()
			return templResponse, nil
		}
		groups, err := n.storageMan.GetGroupRepository().GetAll()

		snapshot := struct {
			Users    []*entity.User      `json:"users"`
			Messages []*entity.Message   `json:"messages"`
			Groups   []*entity.ChatGroup `json:"groups"`
		}{
			Users:    users,
			Messages: messages,
			Groups:   groups,
		}

		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(snapshot)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = n.storageMan.GetCachedEpoch()
		return templResponse, nil

	})
	n.logf("main", "Done registering")
}

// RegisterReaderCallbacks registers functions that handle the forwarder requests, and sync messages, by redirecting them to the appropriate service
func (n *ClusterNode) RegisterReaderCallbacks(as service.AuthService, us service.UserService, ms service.MessageService, gs service.GroupService) {
	n.logf("main", "Registering callback for %d", protocol.ActionUserLogin)
	n.dataMan.RegisterRequestHandler(protocol.ActionUserLogin, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var credentials map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &credentials); err != nil {
			return nil, err
		}

		user, okU := credentials["username"]
		tag, okT := credentials["tag"]
		passw, okH := credentials["password"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okU && okT && okH) {
			return templResponse, nil
		}

		u, newEpoch, err := as.Login(user, tag, passw, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionUserRegister)
	n.dataMan.RegisterSyncHandler(protocol.ActionUserRegister, func(msg *protocol.DataMessage) error {
		var user entity.User
		if err := json.Unmarshal([]byte(msg.Payload), &user); err != nil {
			return err
		}

		_, err := n.storageMan.GetUserRepository().SynCreate(&user, msg.Epoch)
		return err
	})
	n.logf("main", "Registering callback for %d", protocol.ActionUserGetNameTag)
	n.dataMan.RegisterRequestHandler(protocol.ActionUserGetNameTag, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var searchTags map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &searchTags); err != nil {
			return nil, err
		}

		user, okU := searchTags["username"]
		tag, okT := searchTags["tag"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okU && okT) {
			return templResponse, nil
		}

		u, newEpoch, err := us.GetUserByNameTag(user, tag, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionUsersGetName)
	n.dataMan.RegisterRequestHandler(protocol.ActionUsersGetName, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {

		username := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		u, newEpoch, err := us.GetUsersByName(username, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionUserDelete)
	n.dataMan.RegisterSyncHandler(protocol.ActionUserDelete, func(msg *protocol.DataMessage) error {
		uuid := string(msg.Payload)

		_, err := n.storageMan.GetUserRepository().SynSoftDelete(uuid, msg.Epoch)
		return err
	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionMessageSend)
	n.dataMan.RegisterSyncHandler(protocol.ActionMessageSend, func(msg *protocol.DataMessage) error {
		var message entity.Message
		if err := json.Unmarshal(msg.Payload, &message); err != nil {
			return err
		}

		_, err := n.storageMan.GetMessageRepository().SynCreate(&message, msg.Epoch)
		return err
	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionGroupMessageSend)
	n.dataMan.RegisterSyncHandler(protocol.ActionGroupMessageSend, func(msg *protocol.DataMessage) error {
		var message entity.Message
		if err := json.Unmarshal(msg.Payload, &message); err != nil {
			return err
		}

		_, err := n.storageMan.GetMessageRepository().SynCreate(&message, msg.Epoch)
		return err
	})
	n.logf("main", "Registering callback for %d", protocol.ActionMessageRecv)
	n.dataMan.RegisterRequestHandler(protocol.ActionMessageRecv, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		var params map[string]string
		if err := json.Unmarshal([]byte(msg.Payload), &params); err != nil {
			return nil, err
		}

		sender, okS := params["sender"]
		receiver, okR := params["receiver"]

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		if !(okS && okR) {
			return templResponse, nil
		}

		msgs, newEpoch, err := ms.GetDM(sender, receiver, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(msgs)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMessageRecv)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMessageRecv, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {

		group := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		msgs, newEpoch, err := ms.GetGroup(group, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(msgs)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil

	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionGroupCreate)
	n.dataMan.RegisterSyncHandler(protocol.ActionGroupCreate, func(msg *protocol.DataMessage) error {
		var group entity.ChatGroup
		err := json.Unmarshal(msg.Payload, &group)
		if err != nil {
			return err
		}
		_, err = n.storageMan.GetGroupRepository().SynCreate(&group, msg.Epoch)
		return err
	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionGroupDelete)
	n.dataMan.RegisterSyncHandler(protocol.ActionGroupDelete, func(msg *protocol.DataMessage) error {
		groupUuid := string(msg.Payload)

		_, err := n.storageMan.GetGroupRepository().SynSoftDelete(groupUuid, msg.Epoch)
		return err
	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionGroupMemberJoin)
	n.dataMan.RegisterSyncHandler(protocol.ActionGroupMemberJoin, func(msg *protocol.DataMessage) error {
		var params map[string]string
		if err := json.Unmarshal(msg.Payload, &params); err != nil {
			return err
		}

		groupUuid, okG := params["group-uuid"]
		userUuid, okU := params["user-uuid"]
		if !(okG && okU) {
			return fmt.Errorf("Malformatted payload")
		}

		user, err := n.storageMan.GetUserRepository().GetByUUID(userUuid)
		if err != nil {
			return err
		}

		_, err = n.storageMan.GetGroupRepository().SynAddUser(groupUuid, user, msg.Epoch)
		return err
	})
	n.logf("main", "Registering sync callback for %d", protocol.ActionGroupMemberRemove)
	n.dataMan.RegisterSyncHandler(protocol.ActionGroupMemberRemove, func(msg *protocol.DataMessage) error {
		var params map[string]string
		if err := json.Unmarshal(msg.Payload, &params); err != nil {
			return err
		}

		groupUuid, okG := params["group-uuid"]
		userUuid, okU := params["user-uuid"]
		if !(okG && okU) {
			return fmt.Errorf("Malformatted payload")
		}

		_, err := n.storageMan.GetGroupRepository().SynRemoveUser(groupUuid, userUuid, msg.Epoch)
		return err
	})
	n.logf("main", "Registering callback for %d", protocol.ActionGroupMembersGet)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupMembersGet, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		groupUuid := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		members, newEpoch, err := gs.GetGroupMembers(groupUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, _ := json.Marshal(members)
		templResponse.Payload = payload

		templResponse.Epoch = newEpoch

		return templResponse, nil
	})

	n.logf("main", "Registering callback for %d", protocol.ActionGroupGetUUID)
	n.dataMan.RegisterRequestHandler(protocol.ActionGroupGetUUID, func(msg *protocol.DataMessage) (*protocol.DataMessage, error) {
		groupUuid := string(msg.Payload)

		originalSender := msg.OriginNode

		templResponse := protocol.NewResponseMessage(
			n.NewMessageHeader(originalSender, protocol.Data),
			msg.MessageID,
			n.GetId(),      // I am new origin
			originalSender, // Original sender is new final destination
			protocol.FAILURE,
			msg.Action,
			"Missing parameters",
			[]byte{},
			msg.Epoch,
		)

		group, newEpoch, err := gs.GetGroupByUUID(groupUuid, msg.Epoch)
		if err != nil {
			templResponse.ErrorMessage = "Wrong credentials."
			return templResponse, nil
		}
		templResponse.Status = protocol.SUCCESS
		templResponse.ErrorMessage = ""

		payload, err := json.Marshal(group)
		if err != nil {
			return nil, err
		}

		templResponse.Payload = payload
		templResponse.Epoch = newEpoch

		return templResponse, nil
	})

	n.logf("main", "Registering warmup callback")
	n.dataMan.SetWarmupCallback(func(syn *protocol.DataMessage) error {

		var snapshot struct {
			Users    []*entity.User      `json:"users"`
			Messages []*entity.Message   `json:"messages"`
			Groups   []*entity.ChatGroup `json:"groups"`
		}

		if err := json.Unmarshal([]byte(syn.Payload), &snapshot); err != nil {
			return err
		}

		if err := n.storageMan.GetGlobalRepository().SyncSnapshot(syn.Epoch, snapshot.Users, snapshot.Messages, snapshot.Groups); err != nil {
			return err
		}
		n.storageMan.UpdateEpochCache(syn.Epoch)

		return nil

	})
	n.logf("main", "Done registering")
}

//============================================================================//
//  StorageManager                                                            //
//============================================================================//

// Starts the storage manager
func (n *ClusterNode) setupStorageManager() {
	if n.storageMan == nil {
		db, err := gorm.Open(sqlite.Open(n.config.FolderPath+"/"+n.config.DBName), &gorm.Config{})
		if err != nil {
			n.logf("main", "FATAL: Database could not be opened correctly")
			n.cancel()
			return
		}
		db.AutoMigrate(
			&entity.SystemState{},
			&entity.User{},
			&entity.ChatGroup{},
			&entity.UserSecret{},
			&entity.Message{},
		)
		n.storageMan = data.NewStorageManager(db)
	}
}

// getGlobalRepo returns the global repository of storage manager
func (n *ClusterNode) getGlobalRepo() repository.GlobalRepository {
	return n.storageMan.GetGlobalRepository()
}

//============================================================================//
//  Wrappers for NodeConfig component                                         //
//============================================================================//

// GetId returns the ID of the node.
func (n *ClusterNode) GetId() node.NodeId {
	return node.NodeId(n.config.NodeId)
}

// GetControlPort returns the port of the node used for the control plane.
func (n *ClusterNode) GetControlPort() uint16 {
	return n.config.ControlPlanePort
}

// GetDataPort returns the port of the node used for the data plane.
func (n *ClusterNode) GetDataPort() uint16 {
	return n.config.DataPlanePort
}

//============================================================================//
//  Wrappers for Protocol component                                           //
//============================================================================//

// newMessageHeader creates a preconfigured message header for the given neighbor (destination) and type.
// The timestamp field is empty, it will be automatically marked when Sent (for accuracy)
func (n *ClusterNode) NewMessageHeader(dest node.NodeId, typ protocol.MessageScope) *protocol.MessageHeader {
	return protocol.NewMessageHeader(
		n.GetId().Identifier(),
		dest.Identifier(),
		typ,
	)
}
