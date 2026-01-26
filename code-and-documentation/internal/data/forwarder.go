/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package data

import "server/cluster/node/protocol"

// Interface used for the Proxy implementation of the services. A Forwarder knows how to, and can, forward the request to a node that can handle it.
type Forwarder interface {

	// Executes an operation on a remote node, returning the response.
	// It takes a request ID, an action, epoch and a payload, creating a request that is sent towards a capable node.
	// A response is returned, either successful or not, or if not possible (any error that renders the node unable to even craft a response), nil with an error is returned
	ExecuteRemote(id string, action protocol.DataAction, epoch uint64, payload []byte) (*protocol.DataMessage, error)
}
