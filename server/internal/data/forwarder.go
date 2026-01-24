package data

import "server/cluster/node/protocol"

// Interface used for the Proxy implementation of the services. A Forwarder knows how to, and can, forward the request to a node that can handle it.
type Forwarder interface {

	// Executes an operation on a remote node, returning the response.
	// It takes a request ID, an action, epoch and a payload, creating a request that is sent towards a capable node.
	// A response is returned, either successful or not, or if not possible (any error that renders the node unable to even craft a response), nil with an error is returned
	ExecuteRemote(id string, action protocol.DataAction, epoch uint64, payload []byte) (*protocol.DataMessage, error)
}
