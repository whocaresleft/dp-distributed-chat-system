package data

import (
	"encoding/json"
	"fmt"
	"server/cluster/control"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"sync"
	"sync/atomic"
	"time"
)

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

	enstablishedConnections map[node.NodeId]struct{}
}

func NewDataPlaneManager(c node.Connection) *DataPlaneManager {

	d := &DataPlaneManager{
		connMan:                 c,
		pendingRequests:         make(map[string](chan *DataMessage)),
		enstablishedConnections: make(map[node.NodeId]struct{}),
	}
	return d
}

func (d *DataPlaneManager) IsReady() bool {
	return d.logger != nil && d.logicalClock != nil && d.hostFinder != nil && d.runtime != nil
}

func (d *DataPlaneManager) Logf(format string, v ...any) {
	d.Logf(format, v...)
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
}

func (d *DataPlaneManager) SetClock(clock *node.LogicalClock) {
	d.logicalClock = clock
}

func (d *DataPlaneManager) HandleIncomingMessage(message *DataMessage) {
	myId, _ := node.ExtractIdentifier([]byte(d.connMan.GetIdentity()))

	switch message.Type {
	case REQUEST: // Handle it or forward it up?
		if myId == message.DestinationNode && (d.canRead && d.canWrite) { // The request is for me, I am leader
			// Handle
			return
		}
		if message.Action == READ && d.canRead {
			// Handle
			return
		}

		if parent, ok := d.runtime.Load().GetRouting().GetUpstreamNextHop(); ok {
			d.BufferOut(parent, d.CopyMessageWithNewHeader(message, parent))
		}
	case RESPONSE: // Mine or forward it down?

		if myId == message.DestinationNode { // Response is for me, handle it
			d.requestMutex.Lock()
			cha, ok := d.pendingRequests[message.MessageID]
			d.requestMutex.Unlock()

			if ok {
				select {
				case cha <- message:
				default:
				}
			}
			return
		}
		// Forward it down
		if nextHop, ok := d.runtime.Load().GetRouting().GetDownstreamNextHop(message.DestinationNode); ok {
			d.BufferOut(nextHop, d.CopyMessageWithNewHeader(message, nextHop))
		}

	}
}

func (d *DataPlaneManager) ExecuteRemote(id string, payload []byte) (*DataMessage, error) {
	runtime := d.runtime.Load()

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
		WRITE,
		payload,
	)

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
		return response, nil
	case <-time.After(10 * time.Second):
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
	//d.outputChannel <- protocol.OutMessage{DestId: destId, Message: message}
}

func (d *DataPlaneManager) SendTo(destId node.NodeId, message protocol.Message) error {

	h := message.GetHeader()
	h.MarkTimestamp(d.logicalClock.IncrementClock())
	message.SetHeader(h)

	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	d.Logf("About to send the message to %d...", destId)

	if _, ok := d.enstablishedConnections[destId]; !ok {
		host, err := d.hostFinder.GetHost(destId)
		if err != nil {
			return err
		}
		port := d.runtime.Load().GetRouting().GetUpstreamPort()
		d.connMan.ConnectTo(node.FullAddr(host, port))
		d.enstablishedConnections[destId] = struct{}{}
		d.Logf("Connection did not exist... Creating one with (%d => %s:%d)...", destId, host, port)
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
