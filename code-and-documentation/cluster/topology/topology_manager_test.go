/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package topology

import (
	"fmt"
	"server/cluster/node"
	"testing"
	"time"
)

type MockConnection struct {
	inChan, outChan chan [][]byte
}

func (mock *MockConnection) Bind(port uint16) error {
	fmt.Printf("Binding on %d", port)
	return nil
}
func (mock *MockConnection) GetIdentity() string                 { return "" }
func (mock *MockConnection) ConnectTo(address string) error      { return nil }
func (mock *MockConnection) DisconnectFrom(address string) error { return nil }
func (mock *MockConnection) SwitchAddress(old, new string) error { return nil }
func (mock *MockConnection) SendTo(id string, payload []byte) error {
	frame1 := []byte(id)
	frame2 := make([]byte, 0)
	frame3 := payload

	var frames [][]byte = [][]byte{frame1, frame2, frame3}

	mock.outChan <- frames
	return nil
}
func (mock *MockConnection) Recv() ([][]byte, error) {
	return <-mock.inChan, nil
}
func (mock *MockConnection) Poll(timeout time.Duration) error {
	return nil
}
func (mock *MockConnection) Destroy() {

}

func TestNeighborSendCorrect(t *testing.T) {

	oneToTwo := make(chan [][]byte, 10)
	twoToOne := make(chan [][]byte, 10)

	m1 := &MockConnection{
		inChan:  twoToOne,
		outChan: oneToTwo,
	}

	m2 := &MockConnection{
		inChan:  oneToTwo,
		outChan: twoToOne,
	}

	topologyMan1, _ := NewTopologyManager(m1, 46001)
	topologyMan2, _ := NewTopologyManager(m2, 46002)

	topologyMan1.neighbors[2] = node.Address{Host: "localhost", Port: 46002} // Marking them as neighbors otherwise the messages are not sent.
	topologyMan2.neighbors[1] = node.Address{Host: "localhost", Port: 46001} // Not worth to mock all the handshake for this message

	agreedPayload := "hey"

	go func() {
		_, msg, _ := topologyMan2.Recv()
		if string(msg[0]) != agreedPayload {
			t.Errorf("Expected %s and got %s", agreedPayload, msg[0])
		}
		topologyMan2.SendTo(1, []byte{})
	}()

	topologyMan1.SendTo(2, []byte(agreedPayload))
	topologyMan1.Recv()
}

func TestNeighborSendIncorrect(t *testing.T) {

	oneToTwo := make(chan [][]byte, 10)
	twoToOne := make(chan [][]byte, 10)

	m1 := &MockConnection{
		inChan:  twoToOne,
		outChan: oneToTwo,
	}

	m2 := &MockConnection{
		inChan:  oneToTwo,
		outChan: twoToOne,
	}

	topologyMan1, _ := NewTopologyManager(m1, 46001)
	topologyMan2, _ := NewTopologyManager(m2, 46002)

	topologyMan1.neighbors[2] = node.Address{Host: "localhost", Port: 46002}
	topologyMan2.neighbors[1] = node.Address{Host: "localhost", Port: 46001}

	agreedPayload := "hey"

	go func() {
		_, msg, _ := topologyMan2.Recv()
		if string(msg[0]) == agreedPayload {
			t.Errorf("Wrong, the other neighbor sent something else!")
		}
		topologyMan2.SendTo(1, []byte{})
	}()

	topologyMan1.SendTo(2, []byte(agreedPayload+"salt"))
	topologyMan1.Recv()
}
