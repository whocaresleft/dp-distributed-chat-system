/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package node

import (
	"testing"
)

func TestNodeConfigIncorrectControl(t *testing.T) {
	id := NodeId(1)
	incorrectPort, correctPort := 45000, 46000

	_, err := NewNodeConfig(id, incorrectPort, correctPort)
	if err == nil {
		t.Errorf("Expected error...")
	}

	expected := "The port 45000 is outside valid range: [46000, 65535]"
	if err.Error() != expected {
		t.Errorf("Another error was supposed to happen. GOT[%s], EXPECTED[%s]", err.Error(), expected)
	}
}

func TestNodeConfigIncorrectData(t *testing.T) {
	id := NodeId(1)
	incorrectPort, correctPort := 45000, 46000

	_, err := NewNodeConfig(id, correctPort, incorrectPort)
	if err == nil {
		t.Errorf("Expected error...")
	}

	expected := "The port 45000 is outside valid range: [46000, 65535]"
	if err.Error() != expected {
		t.Errorf("Another error was supposed to happen. GOT[%s], EXPECTED[%s]", err.Error(), expected)
	}
}

func TestNodeConfigIncorrectSamePort(t *testing.T) {
	id := NodeId(1)
	port := 46000

	_, err := NewNodeConfig(id, port, port)
	if err == nil {
		t.Errorf("Expected error...")
	}

	expected := "Cannot use the same port for data and control plane"
	if err.Error() != expected {
		t.Errorf("Another error was supposed to happen. GOT[%s], EXPECTED[%s]", err.Error(), expected)
	}
}

func TestNodeConfigCorrectCreation(t *testing.T) {
	id := NodeId(1)
	port1, port2 := 46001, 50001

	_, err := NewNodeConfig(id, port1, port2)
	if err != nil {
		t.Errorf("Expected no error...")
	}
}
