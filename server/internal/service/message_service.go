package service

import (
	"encoding/json"
	"fmt"
	"server/cluster/nlog"
	"server/internal/data"
	"server/internal/entity"
	"server/internal/repository"
	"time"

	"github.com/google/uuid"
)

type MessageService interface {
	CreateMessage(sender, receiver, content string, lastEpoch uint64) (*entity.Message, uint64, error)
	Get(sender, receiver string, lastEpoch uint64) ([]*entity.Message, uint64, error)
}

type proxyMessageService struct {
	forwarder data.Forwarder
	logger    nlog.Logger
}

func NewProxyMessageService(forwarder data.Forwarder, logger nlog.Logger) MessageService {
	return &proxyMessageService{
		forwarder: forwarder,
		logger:    logger,
	}
}

func (m *proxyMessageService) Logf(format string, v ...any) {
	m.logger.Logf(format, v...)
}

func (m *proxyMessageService) CreateMessage(sender, receiver, content string, lastEpoch uint64) (*entity.Message, uint64, error) {
	m.Logf("Forwarding the request")
	payload, err := json.Marshal(map[string]string{
		"sender":   sender,
		"receiver": receiver,
		"content":  content,
	})
	if err != nil {
		return nil, 0, err
	}

	response, err := m.forwarder.ExecuteRemote(uuid.New().String(), data.ActionMessageSend, lastEpoch, payload)
	if err != nil {
		m.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != data.SUCCESS {
		s := fmt.Errorf("User was not found. %v", response.ErrorMessage)
		m.Logf("%v", s.Error())
		return nil, 0, s
	}

	var msg entity.Message
	if err := json.Unmarshal(response.Payload, &msg); err != nil {
		return nil, 0, err
	}

	m.Logf("Message created correctly")
	return &msg, response.Epoch, nil
}
func (m *proxyMessageService) Get(sender, receiver string, lastEpoch uint64) ([]*entity.Message, uint64, error) {
	m.Logf("Forwarding the request")
	payload, err := json.Marshal(map[string]string{
		"sender":   sender,
		"receiver": receiver,
	})

	response, err := m.forwarder.ExecuteRemote(uuid.New().String(), data.ActionMessageRecv, lastEpoch, payload)
	if err != nil {
		m.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != data.SUCCESS {
		s := fmt.Errorf("Messages were not found. %v", response.ErrorMessage)
		m.Logf("%v", s.Error())
		return nil, 0, s
	}

	var msgs []*entity.Message
	if err := json.Unmarshal(response.Payload, &msgs); err != nil {
		return nil, 0, err
	}

	m.Logf("Messages retrieved correctly")
	return msgs, response.Epoch, nil
}

type localMessageService struct {
	canWrite          bool
	forwarder         data.Forwarder
	logger            nlog.Logger
	messageRepository repository.MessageRepository
	globalRepository  repository.GlobalRepository
}

func NewLocalMessageService(canWrite bool, messageRepo repository.MessageRepository, globalRepo repository.GlobalRepository, forwarder data.Forwarder, logger nlog.Logger) MessageService {
	return &localMessageService{
		canWrite:          canWrite,
		forwarder:         forwarder,
		logger:            logger,
		globalRepository:  globalRepo,
		messageRepository: messageRepo,
	}
}

func (m *localMessageService) Logf(format string, v ...any) {
	m.logger.Logf(format, v...)
}

func (m *localMessageService) CreateMessage(sender, receiver, content string, _ uint64) (*entity.Message, uint64, error) {
	if m.canWrite {
		m.Logf("I Have READ-WRITE permissions, proceeding with request")

		message := &entity.Message{
			ChatID:       getChatID(sender, receiver),
			Content:      content,
			Epoch:        0,
			CreatedAt:    time.Now(),
			SenderUUID:   sender,
			ReceiverUUID: receiver,
		}
		newEpoch, err := m.messageRepository.Create(message)
		if err != nil {
			return nil, 0, err
		}
		m.Logf("Message creation outcome {%v, %d}", *message, newEpoch)
		return message, newEpoch, nil
	}
	return nil, 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}

func (m *localMessageService) Get(sender, receiver string, _ uint64) ([]*entity.Message, uint64, error) {
	m.Logf("Local DB found, proceeding...")
	messages, err := m.messageRepository.Get(getChatID(sender, receiver))
	if err != nil {
		return nil, 0, fmt.Errorf("No messages between these users %v", err.Error())
	}
	m.Logf("Found %d users", len(messages))

	epoch, err := m.globalRepository.GetCurrentEpoch()
	if err != nil {
		epoch = 0
	}

	return messages, epoch, nil
}

func getChatID(sender, receiver string) string {
	var finalId string
	if sender < receiver {
		finalId = sender + ":" + receiver
	} else {
		finalId = receiver + ":" + sender
	}

	return finalId
}
