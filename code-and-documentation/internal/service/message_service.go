/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package service

import (
	"encoding/json"
	"fmt"
	"server/cluster/nlog"
	"server/cluster/node/protocol"
	"server/internal/data"
	"server/internal/entity"
	"server/internal/repository"
	"time"

	"github.com/google/uuid"
)

// Service used to handle messages, both for DM and group chats
type MessageService interface {
	CreateDMMessage(sender, receiver, content string, lastEpoch uint64) (*entity.Message, uint64, error) // Creates a message between two users, sender and receiver
	GetDM(sender, receiver string, lastEpoch uint64) ([]*entity.Message, uint64, error)                  // Retrieves messages the chat between two users

	CreateGroupMessage(sender, group, content string, lastEpoch uint64) (*entity.Message, uint64, error) // Creates a message inside a group chat
	GetGroup(group string, lastEpoch uint64) ([]*entity.Message, uint64, error)                          // Retrieves the messages in the group chat.
}

// Proxy is the implementation of the service on the input nodes.
// Since they hold no database, they can only forward the request towards a node that can handle it.
type proxyMessageService struct {
	forwarder data.Forwarder // Forwards the requests and returns the responses
	logger    nlog.Logger    // Logs a format string
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

func (m *proxyMessageService) CreateDMMessage(sender, receiver, content string, lastEpoch uint64) (*entity.Message, uint64, error) {
	m.Logf("Forwarding the request")
	payload, err := json.Marshal(map[string]string{
		"sender":   sender,
		"receiver": receiver,
		"content":  content,
	})
	if err != nil {
		return nil, 0, err
	}

	response, err := m.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionMessageSend, lastEpoch, payload)
	if err != nil {
		m.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Message was not created. %v", response.ErrorMessage)
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

func (m *proxyMessageService) CreateGroupMessage(sender, group, content string, lastEpoch uint64) (*entity.Message, uint64, error) {
	m.Logf("Forwarding the request")
	payload, err := json.Marshal(map[string]string{
		"sender":   sender,
		"receiver": group,
		"content":  content,
	})
	if err != nil {
		return nil, 0, err
	}

	response, err := m.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupMessageSend, lastEpoch, payload)
	if err != nil {
		m.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Message was not created. %v", response.ErrorMessage)
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

func (m *proxyMessageService) GetDM(sender, receiver string, lastEpoch uint64) ([]*entity.Message, uint64, error) {
	m.Logf("Forwarding the request")
	payload, err := json.Marshal(map[string]string{
		"sender":   sender,
		"receiver": receiver,
	})

	response, err := m.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionMessageRecv, lastEpoch, payload)
	if err != nil {
		m.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
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
func (m *proxyMessageService) GetGroup(group string, lastEpoch uint64) ([]*entity.Message, uint64, error) {
	m.Logf("Forwarding the request")
	payload := []byte(group)

	response, err := m.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupMessageRecv, lastEpoch, payload)
	if err != nil {
		m.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
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

// Local service is the implementation of the service on the persistence nodes.
// If the service is WRITE-ENABLED, he can write to the database.
// Otherwise, it can only read from it's replica, only if it's up to date (this endpoins is reachable only if write-enabled).
type localMessageService struct {
	canWrite          bool                         // Is this service node write enables?
	logger            nlog.Logger                  // Logs a format string
	messageRepository repository.MessageRepository // Repository for messages
	globalRepository  repository.GlobalRepository  // Repository for a global stsate
}

func NewLocalMessageService(canWrite bool, messageRepo repository.MessageRepository, globalRepo repository.GlobalRepository, logger nlog.Logger) MessageService {
	return &localMessageService{
		canWrite:          canWrite,
		logger:            logger,
		globalRepository:  globalRepo,
		messageRepository: messageRepo,
	}
}

func (m *localMessageService) Logf(format string, v ...any) {
	m.logger.Logf(format, v...)
}

func (m *localMessageService) CreateDMMessage(sender, receiver, content string, _ uint64) (*entity.Message, uint64, error) {
	if m.canWrite {
		m.Logf("I Have READ-WRITE permissions, proceeding with request")

		message := &entity.Message{
			UUID:         uuid.New().String(),
			ChatID:       getChatID(sender, receiver),
			Content:      content,
			Epoch:        0,
			CreatedAt:    time.Now(),
			SenderUUID:   sender,
			ReceiverUUID: receiver,
			IsForGroup:   false,
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

func (m *localMessageService) CreateGroupMessage(sender, group, content string, _ uint64) (*entity.Message, uint64, error) {
	if m.canWrite {
		m.Logf("I Have READ-WRITE permissions, proceeding with request")

		message := &entity.Message{
			UUID:         uuid.New().String(),
			ChatID:       group,
			Content:      content,
			Epoch:        0,
			CreatedAt:    time.Now(),
			SenderUUID:   sender,
			ReceiverUUID: group,
			IsForGroup:   true,
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

func (m *localMessageService) GetDM(sender, receiver string, _ uint64) ([]*entity.Message, uint64, error) {
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

func (m *localMessageService) GetGroup(group string, _ uint64) ([]*entity.Message, uint64, error) {
	m.Logf("Local DB found, proceeding...")
	messages, err := m.messageRepository.Get(group)
	if err != nil {
		return nil, 0, fmt.Errorf("No messages in this chat %v", err.Error())
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
