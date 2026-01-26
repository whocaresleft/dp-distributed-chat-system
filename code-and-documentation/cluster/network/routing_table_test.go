/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package network

import (
	"server/cluster/node"
	"testing"
)

func TestUnsetUpstreamUsage(t *testing.T) {
	r1 := NewRoutingTable()
	_, ok := r1.GetUpstreamNextHop()
	if ok {
		t.Errorf("Upstream next hop should not be set, this sohuld be false")
	}
}
func TestUnsetDownstreamUsage(t *testing.T) {
	r1 := NewRoutingTable()

	r1.SetDownstreamNextHop(2, 1)

	_, ok := r1.GetDownstreamNextHop(2)
	if !ok {
		t.Errorf("Downstream next hop should be set, this sohuld be true")
	}

	_, ok = r1.GetDownstreamNextHop(3)
	if ok {
		t.Errorf("Downstream next hop should not be set, this sohuld be false")
	}
}

func TestCorrectUpstreamUsage(t *testing.T) {

	up := node.NodeId(1)

	r1 := NewRoutingTable()
	r1.SetUpstreamNextHop(up)

	nextHop, ok := r1.GetUpstreamNextHop()
	if !ok {
		t.Errorf("Upstream next hop should not be set, this sohuld be true")
	}
	if nextHop != up {
		t.Errorf("Upstream next hop should be %d, not %d", up, nextHop)
	}
}
func TestCorrectDownstreamUsage(t *testing.T) {
	r1 := NewRoutingTable()

	down, nextHop := node.NodeId(2), node.NodeId(1)

	r1.SetDownstreamNextHop(down, nextHop)

	nh, ok := r1.GetDownstreamNextHop(down)
	if !ok {
		t.Errorf("Downstream next hop should be set, this sohuld be true")
	}
	if nh != nextHop {
		t.Errorf("Downstream next hop should be %d=>%d, not %d=>%d", down, nextHop, down, nh)
	}
}

func TestDeepClone(t *testing.T) {
	r1 := NewRoutingTable()
	r1.SetUpstreamPort(50000)
	r1.SetUpstreamNextHop(4)
	r1.SetDownstreamNextHop(1, 2)
	r1.SetDownstreamNextHop(6, 7)
	r1.SetDownstreamNextHop(5, 3)

	r2 := r1.Clone()

	if r1.GetUpstreamPort() != r2.GetUpstreamPort() {
		t.Errorf("Port was not cloned")
	}

	up1, ok1 := r1.GetUpstreamNextHop()
	up2, ok2 := r2.GetUpstreamNextHop()

	if !(up1 == up2 && ok1 == ok2) {
		t.Errorf("Upstream next hop was not cloned")
	}

	for key := range r1.GetDownstreamsMap() {
		d1, ok1 := r1.GetDownstreamNextHop(key)
		d2, ok2 := r2.GetDownstreamNextHop(key)

		if !(d1 == d2 && ok1 == ok2) {
			t.Errorf("Downstream next hop was not cloned")
		}
	}
}
