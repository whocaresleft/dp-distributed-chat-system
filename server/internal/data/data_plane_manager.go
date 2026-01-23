package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"server/cluster/control"
	"server/cluster/network"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"sync"
	"sync/atomic"
	"time"
)

type RequestHandler func(msg *DataMessage) (*DataMessage, error)
type SyncHandler func(msg *DataMessage) error

type EpochCache interface {
	GetCachedEpoch() uint64
	UpdateEpochCache(uint64)
}

type DataPlaneManager struct {
	canWrite bool
	canRead  bool

	connMan         node.Connection
	pendingRequests map[string](chan *DataMessage)
	requestMutex    sync.RWMutex

	logger       nlog.Logger
	logicalClock *node.LogicalClock

	hostFinder node.HostFinder
	runtime    *atomic.Pointer[control.RuntimeContext]

	epochCache EpochCache

	inbox                   chan *DataMessage
	outputChannel           chan protocol.OutMessage
	enstablishedConnections map[node.NodeId]struct{}

	running          atomic.Bool
	internalStopChan chan struct{}

	requestHandlers map[Action]RequestHandler
	syncHandlers    map[Action]SyncHandler
}

func NewDataPlaneManager(c node.Connection) *DataPlaneManager {

	c.(*network.ConnectionManager).StartMonitoring(func(s string) {
		fmt.Printf("%s is ON", s)
	})

	d := &DataPlaneManager{
		connMan:                 c,
		pendingRequests:         make(map[string](chan *DataMessage)),
		enstablishedConnections: make(map[node.NodeId]struct{}),
		inbox:                   make(chan *DataMessage, 500),
		outputChannel:           make(chan protocol.OutMessage, 500),
		requestHandlers:         make(map[Action]RequestHandler),
		syncHandlers:            make(map[Action]SyncHandler),
		running:                 atomic.Bool{},
		internalStopChan:        make(chan struct{}, 1),
	}
	return d
}

func (d *DataPlaneManager) IsReady() bool {
	return d.logger != nil && d.logicalClock != nil && d.hostFinder != nil && d.runtime != nil && d.epochCache != nil
}

func (d *DataPlaneManager) Logf(format string, v ...any) {
	d.logger.Logf(format, v...)
}

func (d *DataPlaneManager) SetEpochCacher(e EpochCache) {
	d.epochCache = e
}

func (d *DataPlaneManager) BindPort(port uint16) error {
	if d.connMan == nil {
		return fmt.Errorf("Connection manager is not set on data manager...")
	}
	return d.connMan.Bind(port)
}

func (d *DataPlaneManager) SetRWPermissione(canWrite, canRead bool) {
	d.canRead = canRead
	d.canWrite = canWrite
}

func (d *DataPlaneManager) SetHostFinder(h node.HostFinder) {
	d.hostFinder = h
}

func (d *DataPlaneManager) SetRuntime(r *atomic.Pointer[control.RuntimeContext]) {
	d.runtime = r
}

func (d *DataPlaneManager) SetLogger(l nlog.Logger) {
	d.logger = l
	d.Logf("NIFGAGAG")
}

func (d *DataPlaneManager) SetClock(clock *node.LogicalClock) {
	d.logicalClock = clock
}

func (d *DataPlaneManager) Stop() {
	if d.running.Load() {
		d.internalStopChan <- struct{}{}
		d.running.Store(false)
	}
}

func (d *DataPlaneManager) Run(ctx context.Context) {
	d.Logf("Started data plane manager")

	go d.RunAsyncReceiver(ctx)
	go d.RunAsyncSender(ctx)
	go d.HandleIncomingMessage(ctx)
	d.running.Store(true)
}

func (d *DataPlaneManager) RunAsyncReceiver(ctx context.Context) {
	d.Logf("Started async receiver")
	for {
		select {
		case <-ctx.Done():
			d.Logf("Receiver: Stop signal received")
			return
		case <-d.internalStopChan:
			d.Logf("Sender: Internal stop signal received")
			return
		default:
		}

		if err := d.connMan.Poll(500 * time.Millisecond); err != nil {
			if IsRecvNotReadyError(err) {
				continue
			}
			d.Logf("Polling error %v:", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		id, msg, err := d.Recv()
		if err != nil {
			if IsRecvNotReadyError(err) {
				d.Logf("Recv not ready: %v", err)
				continue
			}
			d.Logf("Message received on dispatcher with an error: %v", err)
			continue
		}

		head := msg.GetHeader()
		d.Logf("Message received on receiver: From %d, %v", id, head.String())

		if head.Type == protocol.Data {
			if m, ok := msg.(*DataMessage); ok {
				d.inbox <- m
			}
		}
	}
}

func (d *DataPlaneManager) RegisterRequestHandler(action Action, handler RequestHandler) {
	d.requestHandlers[action] = handler
}

func (d *DataPlaneManager) RegisterSyncHandler(action Action, handler SyncHandler) {
	d.syncHandlers[action] = handler
}

func IsRecvNotReadyError(err error) bool {
	return errors.Is(err, network.ErrRecvNotReady)
}

func (d *DataPlaneManager) RunAsyncSender(ctx context.Context) {
	d.Logf("Started async sender")
	for {

		select {
		case <-ctx.Done():
			d.Logf("Sender: Stop signal received")
			return
		case <-d.internalStopChan:
			d.Logf("Sender: Internal stop signal received")
			return
		case m := <-d.outputChannel:
			d.SendTo(m.DestId, m.Message)
		}
	}
}

func (d *DataPlaneManager) HandleIncomingMessage(ctx context.Context) {

	myId, _ := node.ExtractIdentifier([]byte(d.connMan.GetIdentity()))

	d.Logf("Started message handle")
	for {
		select {
		case <-ctx.Done():
			d.Logf("Message handle: Stop signal received")
			return
		case <-d.internalStopChan:
			d.Logf("Sender: Internal stop signal received")
			return
		case message := <-d.inbox:
			switch message.Type {
			case REQUEST: // Handle it or forward it up?
				d.Logf("I received a request from %s: %v", message.Header.Sender, message.String())
				if myId == message.DestinationNode && (d.canRead && d.canWrite) { // The request is for me
					d.Logf("This message is for me!")
					// Handle
					response, err := d.HandleRequest(message)
					if err != nil {
						d.Logf("Error occurred in request: %v", err)
						continue
					}
					d.Logf("Response created: %v", response)
					d.SendDown(response.DestinationNode, response)

					if ActionRWFlags[response.Action] == WRITE {
						d.Logf("Sending sync to nexthops: %v", response)
						d.BroadcastSync(response)
					}
					continue
				}
				if ActionRWFlags[message.Action] == READ && d.canRead { // Not for me, but I could handle it with my replica
					epoch := d.epochCache.GetCachedEpoch()
					if message.Epoch > 0 && message.Epoch <= epoch {
						d.Logf("This message is not for me but i can handle it (read request and up to date)!")
						// Handle
						response, err := d.HandleRequest(message)
						if err != nil {
							d.Logf("Error occurred in request: %v", err)
							continue
						}
						d.Logf("Response created: %v", response)
						d.SendDown(response.DestinationNode, response)
						continue
					}
					d.Logf("This message is not for me, i could handle it  but I'm not up to date")
				}

				if err := d.SendUp(message); err != nil {
					d.Logf("Error occurred: %v", err)
					continue
				}
				d.Logf("Message forwarded UP")
			case RESPONSE: // Mine or forward it down?
				d.Logf("I received a response from %s: %v", message.Header.Sender, message.String())

				if myId == message.DestinationNode { // Response is for me, handle it
					d.Logf("This message is for me, send it to its channel")
					d.requestMutex.Lock()
					cha, ok := d.pendingRequests[message.MessageID]
					d.requestMutex.Unlock()

					if ok {
						select {
						case cha <- message:
						default:
						}
					}
					continue
				}
				// Forward it down
				if err := d.SendDown(message.DestinationNode, message); err != nil {
					d.Logf("Error occurred: %v", err)
					continue
				}
				d.Logf("Message forwarder DOWN")
			case SYNC:
				if d.canRead { // Having read pemissions means that I should sync the updates given by the writer
					d.Logf("I received a Sync message from %s: %v", message.Header.Sender, message.String())
					if err := d.HandleSync(message); err != nil {
						d.Logf("ERROR SYNC: %v", err)
						continue
					}
					d.SendDownToAll(message)
				}
			}
		}
	}
}

func (d *DataPlaneManager) BroadcastSync(rep *DataMessage) {
	syncMsg := rep.Clone().(*DataMessage)
	syncMsg.Type = SYNC
	d.SendDownToAll(syncMsg)
}

func (d *DataPlaneManager) HandleSync(syn *DataMessage) error {
	handler, ok := d.syncHandlers[syn.Action]
	if !ok {
		return fmt.Errorf("No handler registered for opertation {%v}.", syn.Action)
	}
	if err := handler(syn); err != nil {
		return err
	}
	old := d.epochCache.GetCachedEpoch()
	d.epochCache.UpdateEpochCache(syn.Epoch)
	new := d.epochCache.GetCachedEpoch()
	d.Logf("CALLING UPDATE CACHE, OLD VAL: %d, NEW VAL: %d", old, new)
	return nil
}

func (d *DataPlaneManager) HandleRequest(req *DataMessage) (rep *DataMessage, err error) {
	handler, ok := d.requestHandlers[req.Action]
	if !ok {
		return nil, fmt.Errorf("No handler registered for opertation {%v}.", req.Action)
	}
	return handler(req)
}

func (d *DataPlaneManager) SendUp(original protocol.Message) error {
	nextHop, ok := d.runtime.Load().GetRouting().GetUpstreamNextHop()
	if !ok {
		return fmt.Errorf("No upstream next hop is set")
	}
	new := d.CopyMessageWithNewHeader(original, nextHop)
	d.Logf("Next hop for upstream is %d", nextHop)
	d.BufferOut(nextHop, new)
	return nil
}

func (d *DataPlaneManager) SendDown(destination node.NodeId, original protocol.Message) error {
	nextHop, ok := d.runtime.Load().GetRouting().GetDownstreamNextHop(destination)
	if !ok {
		return fmt.Errorf("The downstream node %d has no next hop", destination)
	}

	new := d.CopyMessageWithNewHeader(original, nextHop)
	d.Logf("Next hop for downstream %d is %d", destination, nextHop)
	d.BufferOut(nextHop, new)
	return nil
}

func (d *DataPlaneManager) SendDownToAll(original protocol.Message) {
	nextHops := d.runtime.Load().GetRouting().GetDownstreamHops()
	for nextHop := range nextHops {
		new := d.CopyMessageWithNewHeader(original, nextHop)
		d.Logf("Sending to nextHop %d", nextHop)
		d.BufferOut(nextHop, new)
	}
}

func (d *DataPlaneManager) ExecuteRemote(id string, action Action, epoch uint64, payload []byte) (*DataMessage, error) {
	runtime := d.runtime.Load()

	d.Logf("Need to forward the request{Id{%s}, payload{%s}}", id, string(payload))

	parent, ok := runtime.GetRouting().GetUpstreamNextHop()
	if !ok {
		return nil, fmt.Errorf("Request could not be forwarded")
	}

	nodeId, _ := node.ExtractIdentifier([]byte(d.connMan.GetIdentity()))

	request := NewRequestMessage(
		protocol.NewMessageHeader(
			nodeId.Identifier(),
			parent.Identifier(),
			protocol.Data,
		),
		id,
		nodeId,
		runtime.GetLastElection().GetLeaderID(),
		action,

		payload,
		epoch,
	)

	d.Logf("Waiting response for Request{%s}", id)

	responseChannel := make(chan *DataMessage, 1)
	d.requestMutex.Lock()
	d.pendingRequests[id] = responseChannel
	d.requestMutex.Unlock()

	defer func() {
		d.requestMutex.Lock()
		delete(d.pendingRequests, id)
		d.requestMutex.Unlock()
	}()

	d.BufferOut(parent, request)

	select {
	case response := <-responseChannel:
		d.Logf("Received response for Request{%s}: %s", id, response.String())
		return response, nil
	case <-time.After(15 * time.Second):
		d.Logf("Timeout of 15 seconds occurred for Request{%d}", id)
		return nil, fmt.Errorf("Timeout: could not satisfy request in time")
	}
}

func (d *DataPlaneManager) CopyMessageWithNewHeader(message protocol.Message, nextHop node.NodeId) protocol.Message {
	newMsg := message.Clone()
	newMsg.SetHeader(protocol.NewMessageHeader(
		d.connMan.GetIdentity(),
		nextHop.Identifier(),
		protocol.Data,
	))
	return newMsg
}

func (d *DataPlaneManager) BufferOut(destId node.NodeId, message protocol.Message) {
	d.outputChannel <- protocol.OutMessage{DestId: destId, Message: message}
}

func (d *DataPlaneManager) SendTo(destId node.NodeId, message protocol.Message) error {

	h := message.GetHeader()
	h.MarkTimestamp(d.logicalClock.IncrementClock())
	message.SetHeader(h)

	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	d.Logf("About to send the message to %d... %v", destId, d.enstablishedConnections)

	if _, ok := d.enstablishedConnections[destId]; !ok {
		host, err := d.hostFinder.GetHost(destId)
		if err != nil {
			return err
		}
		port := d.runtime.Load().GetRouting().GetUpstreamPort()
		s := node.FullAddr(host, port)
		d.connMan.ConnectTo(s)
		time.Sleep(2 * time.Second)
		d.enstablishedConnections[destId] = struct{}{}
		d.Logf("Connection did not exist... Creating one with (%d => %s:%d)...[%s]", destId, host, port, s)
	}

	d.Logf("Sending to (%s)...", destId.Identifier())
	return d.connMan.SendTo(destId.Identifier(), payload)
}

func (d *DataPlaneManager) Recv() (sourceId node.NodeId, msg protocol.Message, err error) {
	d.Logf("Waiting to recv a message...")
	payload, err := d.connMan.Recv()
	if err != nil {
		return 0, nil, err
	}

	sourceId, _ = node.ExtractIdentifier(payload[0])
	payload = payload[2:]

	d.Logf("Recv'd a message from %d", sourceId)

	var headerWrapper struct {
		Header protocol.MessageHeader `json:"header"`
	}
	if err := json.Unmarshal(payload[0], &headerWrapper); err != nil {
		return 0, nil, err
	}

	header := headerWrapper.Header
	d.logicalClock.UpdateClock(header.TimeStamp)

	switch header.Type {
	case protocol.Data:
		msg = &DataMessage{}
	default:

		return 0, nil, fmt.Errorf("Unknown message type: %v", header.Type)
	}

	if err := json.Unmarshal(payload[0], msg); err != nil {
		return 0, nil, err
	}
	return sourceId, msg, nil
}
