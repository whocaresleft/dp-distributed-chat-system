package nlog

import (
	"fmt"
	"log"
	"os"
	"server/cluster/node"
	"sync"
)

type NodeLogger struct {
	id node.NodeId

	fileMapper map[string]*os.File
	logMapper  map[string]*log.Logger

	lock           sync.RWMutex
	currentLogFunc func(*log.Logger, string, ...any)
}

func NewNodeLogger(id node.NodeId, logging bool) (*NodeLogger, error) {
	if err := os.MkdirAll(fmt.Sprintf("NODE_%d", id), 0755); err != nil {
		return nil, err
	}
	n := &NodeLogger{
		id:             id,
		fileMapper:     make(map[string]*os.File),
		logMapper:      make(map[string]*log.Logger),
		currentLogFunc: nilLogf,
	}

	if logging {
		n.currentLogFunc = defaultLogf
	}

	return n, nil
}

func (n *NodeLogger) AddLogger(filename string) error {
	file, err := os.OpenFile(fmt.Sprintf("NODE_%d/%s.log", n.id, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	n.logMapper[filename] = log.New(file, fmt.Sprintf("[[Node %d] %s]: ", n.id, filename), log.Ldate|log.Ltime)
	n.fileMapper[filename] = file
	return nil
}

func (n *NodeLogger) EnableLogging() {
	n.currentLogFunc = defaultLogf
}
func (n *NodeLogger) DisableLogging() {
	n.currentLogFunc = nilLogf
}

func (n *NodeLogger) Logf(filename, format string, a ...any) error {
	n.lock.Lock()
	logFunc := n.currentLogFunc
	logger, ok := n.logMapper[filename]
	n.lock.Unlock()

	if !ok {
		return fmt.Errorf("Logger is not setup for this filename")
	}
	if logFunc != nil {
		logFunc(logger, format, a...)
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
