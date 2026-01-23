package nlog

import (
	"context"
	"fmt"
	"log"
	"os"
	"server/cluster/node"
	"sync"
)

type Logger interface {
	Logf(format string, v ...any)
}

type subsystemLogger struct {
	filename string
	logger   *NodeLogger
}

func (s *subsystemLogger) Logf(format string, v ...any) {
	s.logger.Logf(s.filename, format, v...)
}

type logEntry struct {
	filename  string
	formatted string
}

type NodeLogger struct {
	id node.NodeId

	fileMapper map[string]*os.File
	logMapper  map[string]*log.Logger

	lock           sync.RWMutex
	clock          *node.LogicalClock
	currentLogFunc func(*log.Logger, string, ...any)

	inbox chan logEntry
}

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

func (n *NodeLogger) GetSubsystemLogger(filename string) (Logger, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	if _, ok := n.logMapper[filename]; !ok {
		return nil, fmt.Errorf("The subsystem was not registered")
	}
	return &subsystemLogger{filename, n}, nil
}

func (n *NodeLogger) EnableLogging() {
	n.lock.Lock()
	n.currentLogFunc = defaultLogf
	n.lock.Unlock()
}
func (n *NodeLogger) DisableLogging() {
	n.lock.Lock()
	n.currentLogFunc = nilLogf
	n.lock.Unlock()
}

func (n *NodeLogger) Logf(filename, format string, v ...any) {
	n.inbox <- logEntry{filename, fmt.Sprintf(fmt.Sprintf("{%d}. %s", n.clock.Snapshot(), format), v...)}
}

func (n *NodeLogger) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-n.inbox:
			n.actualWrite(msg.filename, msg.formatted)
		}
	}
}

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

func defaultLogf(l *log.Logger, format string, a ...any) {
	l.Printf(format, a...)
}

func nilLogf(*log.Logger, string, ...any) {}
