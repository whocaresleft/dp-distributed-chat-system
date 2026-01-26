/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package nlog

import (
	"context"
	"fmt"
	"log"
	"os"
	"server/cluster/node"
	"sync"
)

// Logger is something that can print, using Logf, a format string
type Logger interface {
	Logf(format string, v ...any)
}

// subsystemLogger is a logger that handles only one file out of all that are opened by its logger
type subsystemLogger struct {
	filename string
	logger   *NodeLogger
}

// Logf for a subsystem logger is just a wrap for the Logs of its internal logger, giving its only filename
func (s *subsystemLogger) Logf(format string, v ...any) {
	s.logger.Logf(s.filename, format, v...)
}

// logEntry is an helper struct that can be used to send a couple (filename, formatted string) onto the log channel
type logEntry struct {
	filename  string
	formatted string
}

// NodeLogger is an (almost) powerful logger that can write to multiple log files from one single struct.
// It's safe to share amongst goroutines since it has an internal lock
type NodeLogger struct {
	id node.NodeId // Id of the node, used for the prefix string during logging

	fileMapper map[string]*os.File    // Maps a filename to an OS file (used only to be able to deallocate it later)
	logMapper  map[string]*log.Logger // Maps a filename to the corresponding logger

	lock           sync.RWMutex
	clock          *node.LogicalClock                // logical clock, used for logging timestamps
	currentLogFunc func(*log.Logger, string, ...any) // Current logging function (alternating between defaultLogf and nilLogf)

	inbox chan logEntry // Log channel, formatted strings are sent here instead of directly writing to files
}

// NewNodeLogger Creates and returns a NodeLogger using the given id, logging flag and logical clock
// When successful, error is nil
func NewNodeLogger(id node.NodeId, logging bool, clock *node.LogicalClock) (*NodeLogger, error) {
	if err := os.MkdirAll(fmt.Sprintf("NODE_%d", id), 0755); err != nil {
		return nil, err
	}
	n := &NodeLogger{
		id:             id,
		fileMapper:     make(map[string]*os.File),
		logMapper:      make(map[string]*log.Logger),
		currentLogFunc: nilLogf,
		inbox:          make(chan logEntry, 600),
		clock:          clock,
	}

	if logging {
		n.currentLogFunc = defaultLogf
	}

	return n, nil
}

// RegisterSubsystem registers a new subsystem, returning a Logger that can write to the file filename.
// If successful, error is nil
func (n *NodeLogger) RegisterSubsystem(filename string) (Logger, error) {
	file, err := os.OpenFile(fmt.Sprintf("NODE_%d/%s.log", n.id, filename), os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	n.logMapper[filename] = log.New(file, fmt.Sprintf("[[Node %d] %s]: ", n.id, filename), log.Ldate|log.Ltime)
	n.fileMapper[filename] = file
	return &subsystemLogger{filename, n}, nil
}

// GetSubsystemLogger retrieves a subsystem logger, if previously registerd.
// If successful, error is nil
func (n *NodeLogger) GetSubsystemLogger(filename string) (Logger, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	if _, ok := n.logMapper[filename]; !ok {
		return nil, fmt.Errorf("The subsystem was not registered")
	}
	return &subsystemLogger{filename, n}, nil
}

// EnableLogging enables the logging done by this logger
func (n *NodeLogger) EnableLogging() {
	n.lock.Lock()
	n.currentLogFunc = defaultLogf
	n.lock.Unlock()
}

// DisableLogging disables the logging done by this logger
func (n *NodeLogger) DisableLogging() {
	n.lock.Lock()
	n.currentLogFunc = nilLogf
	n.lock.Unlock()
}

// Logf formats a string using format and v, and appends it to a logging channel, alongside the file, filename, it will be written to
func (n *NodeLogger) Logf(filename, format string, v ...any) {
	n.inbox <- logEntry{filename, fmt.Sprintf(fmt.Sprintf("{%d}. %s", n.clock.Snapshot(), format), v...)}
}

// Run waits either on the log channel or ctx.Done()
// When ctx.Done(), the caller has shut down and we deallocate resources
// When a message arrives on the log channel, we write it accordingly
func (n *NodeLogger) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			n.CloseAll()
			return
		case msg := <-n.inbox:
			n.actualWrite(msg.filename, msg.formatted)
		}
	}
}

// actualWrite is the function that writes the string formatted in the file filename
// When successful, error is nil
func (n *NodeLogger) actualWrite(filename, formatted string) error {
	n.lock.Lock()
	logFunc := n.currentLogFunc
	logger, ok := n.logMapper[filename]
	n.lock.Unlock()

	if !ok {
		return fmt.Errorf("Logger is not setup for this filename")
	}
	if logFunc != nil {
		logFunc(logger, formatted)
	}
	return nil
}

// CloseAll closes all the open files that the loggers are using
func (n *NodeLogger) CloseAll() {
	n.lock.Lock()
	defer n.lock.Unlock()

	for _, file := range n.fileMapper {
		file.Sync()
		file.Close()
	}
	clear(n.fileMapper)
	clear(n.logMapper)
}

// defaultLogf is a log function that writes to a logger l
func defaultLogf(l *log.Logger, format string, a ...any) {
	l.Printf(format, a...)
}

// nilLogf is a log function that does nothing, which gets called when logging is disabled
func nilLogf(*log.Logger, string, ...any) {}
