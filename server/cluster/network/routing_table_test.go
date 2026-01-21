/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package network

import "testing"

func TestUnsetUpstreamUsage(t *testing.T)     {}
func TestUnsetDownstreamUsage(t *testing.T)   {}
func TestCorrectUpstreamUsage(t *testing.T)   {}
func TestCorrectDownstreamUsage(t *testing.T) {}
func TestDeepClone(t *testing.T) {
	r1 := NewRoutingTable()
	r1.SetUpstreamPort(50000)
	r1.SetUpstreamNextHop(4)
	r1.SetDownstreamNextHop(1, 2)
	r1.SetDownstreamNextHop(6, 7)

	r2 := r1.Clone()

	wasItCloned := r1.GetUpstreamPort() == r2.GetUpstreamPort() &&
		r1.GetUpstreamNextHop() == r2.GetUpstreamNextHop() &&
		r1.GetDownstreamNextHop(1) == r2.GetDownstreamNextHop(1) &&
		r1.GetDownstreamNextHop(6) == r2.GetDownstreamNextHop(6)

}
