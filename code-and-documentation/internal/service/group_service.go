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
	"gorm.io/gorm"
)

// Service used to handle groups and user-group interaction
type GroupService interface {
	CreateGroup(name string, creator *entity.User, lastEpoch uint64) (*entity.ChatGroup, uint64, error) // Creates a new group with the given name, adding the creator as first member. It returns the group entity if successful, otherwise nil, with an error, is returned.
	DeleteGroup(uuid string, lastEpoch uint64) (uint64, error)                                          // Deletes the group with the given uuid, if present.

	GetGroupByUUID(uuid string, lastEpoch uint64) (*entity.ChatGroup, uint64, error) // Returns the group entity that has the given uuid. If there is none, nil is returned, with an error.

	GetGroupMembers(uuid string, lastEpoch uint64) ([]*entity.User, uint64, error) // Returns the user entities that are part of the group with the given uuid.
	AddGroupUser(uuid, userUuid string, lastEpoch uint64) (uint64, error)          // Adds a user to a group, both identified via their uuid.
	RemoveGroupUser(uuid, userUuid string, lastEpoch uint64) (uint64, error)       // Removes a user from a group, both identifier via their uuid. The group is deleted if no more users are present
}

// Proxy is the implementation of the service on the input nodes.
// Since they hold no database, they can only forward the request towards a node that can handle it.
type proxyGroupService struct {
	forwarder data.Forwarder // Forwards the requests and returns the responses
	logger    nlog.Logger    // Logs a format string
}

func NewproxyGroupService(forwarder data.Forwarder, logger nlog.Logger) GroupService {
	return &proxyGroupService{
		forwarder: forwarder,
		logger:    logger,
	}
}

func (p *proxyGroupService) Logf(format string, v ...any) {
	p.logger.Logf(format, v...)
}

func (g *proxyGroupService) CreateGroup(name string, creator *entity.User, lastEpoch uint64) (*entity.ChatGroup, uint64, error) {
	g.Logf("Forwarding the  request")

	payload, err := json.Marshal(
		struct {
			GroupName string       `json:"group-name"`
			Creator   *entity.User `json:"creator"`
		}{
			name,
			creator,
		},
	)
	if err != nil {
		return nil, 0, err
	}

	response, err := g.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupCreate, lastEpoch, payload)
	if err != nil {
		g.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Group was not created. %v", response.ErrorMessage)
		g.Logf("%v", s.Error())
		return nil, 0, s
	}

	var group entity.ChatGroup
	if err := json.Unmarshal(response.Payload, &group); err != nil {
		return nil, 0, err
	}

	g.Logf("Group created correctly")
	return &group, response.Epoch, nil
}
func (g *proxyGroupService) DeleteGroup(groupUUID string, lastEpoch uint64) (uint64, error) {
	g.Logf("Forwarding the  request")

	payload := []byte(groupUUID)

	response, err := g.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupDelete, lastEpoch, payload)
	if err != nil {
		g.Logf("Received error on request execution {%v}", err)
		return 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Group was not deleted. %v", response.ErrorMessage)
		g.Logf("%v", s.Error())
		return 0, s
	}

	g.Logf("Group deleted correctly")
	return response.Epoch, nil
}

func (g *proxyGroupService) GetGroupByUUID(groupUUID string, lastEpoch uint64) (*entity.ChatGroup, uint64, error) {
	g.Logf("Forwarding the  request")

	payload := []byte(groupUUID)

	response, err := g.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupGetUUID, lastEpoch, payload)
	if err != nil {
		g.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Group was not found. %v", response.ErrorMessage)
		g.Logf("%v", s.Error())
		return nil, 0, s
	}

	var group entity.ChatGroup
	if err := json.Unmarshal(response.Payload, &group); err != nil {
		return nil, 0, err
	}

	g.Logf("User retrieved correctly")
	return &group, response.Epoch, nil
}

func (g *proxyGroupService) GetGroupMembers(groupUUID string, lastEpoch uint64) ([]*entity.User, uint64, error) {
	g.Logf("Forwarding the  request")

	payload := []byte(groupUUID)

	response, err := g.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupMembersGet, lastEpoch, payload)
	if err != nil {
		g.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Users not found in group. %v", response.ErrorMessage)
		g.Logf("%v", s.Error())
		return nil, 0, s
	}

	var users []*entity.User
	if err := json.Unmarshal(response.Payload, &users); err != nil {
		return nil, 0, err
	}

	g.Logf("Group members retrieved correctly")
	return users, response.Epoch, nil
}
func (g *proxyGroupService) AddGroupUser(groupUuid, userUuid string, lastEpoch uint64) (uint64, error) {
	g.Logf("Forwarding the  request")

	payload, err := json.Marshal(map[string]string{
		"group-uuid": groupUuid,
		"user-uuid":  userUuid,
	})
	if err != nil {
		return 0, err
	}

	response, err := g.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupMemberJoin, lastEpoch, payload)
	if err != nil {
		g.Logf("Received error on request execution {%v}", err)
		return 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("User was not added. %v", response.ErrorMessage)
		g.Logf("%v", s.Error())
		return 0, s
	}

	g.Logf("User added correctly")
	return response.Epoch, nil
}
func (g *proxyGroupService) RemoveGroupUser(groupUuid, userUuid string, lastEpoch uint64) (uint64, error) {
	g.Logf("Forwarding the  request")

	payload, err := json.Marshal(map[string]string{
		"group-uuid": groupUuid,
		"user-uuid":  userUuid,
	})
	if err != nil {
		return 0, err
	}

	response, err := g.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionGroupMemberRemove, lastEpoch, payload)
	if err != nil {
		g.Logf("Received error on request execution {%v}", err)
		return 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("User was not removed. %v", response.ErrorMessage)
		g.Logf("%v", s.Error())
		return 0, s
	}

	g.Logf("User removed correctly")
	return response.Epoch, nil
}

// Local service is the implementation of the service on the persistence nodes.
// If the service is WRITE-ENABLED, he can write to the database.
// Otherwise, it can only read from it's replica, only if it's up to date (this endpoins is reachable only if write-enabled).
type localGroupService struct {
	canWrite         bool                        // Is this service node write enables?
	logger           nlog.Logger                 // Logs a format string
	groupRepository  repository.GroupRepository  // Repository for groups
	userRepository   repository.UserRepository   // Repository for users
	globalRepository repository.GlobalRepository // Repository for a global stsate
}

func NewLocalGroupService(canWrite bool, groupRepo repository.GroupRepository, userRepo repository.UserRepository, globalRepo repository.GlobalRepository, logger nlog.Logger) GroupService {
	return &localGroupService{
		canWrite:         canWrite,
		logger:           logger,
		groupRepository:  groupRepo,
		globalRepository: globalRepo,
		userRepository:   userRepo,
	}
}
func (p *localGroupService) Logf(format string, v ...any) {
	p.logger.Logf(format, v...)
}

func (g *localGroupService) CreateGroup(name string, creator *entity.User, _ uint64) (*entity.ChatGroup, uint64, error) {
	if g.canWrite {
		g.Logf("I Have READ-WRITE permissions, proceeding with request")

		group := &entity.ChatGroup{
			UUID:      uuid.New().String(),
			Name:      name,
			Epoch:     0,
			CreatedAt: time.Now(),
			DeletedAt: gorm.DeletedAt{},
			Members:   []*entity.User{creator},
		}
		newEpoch, err := g.groupRepository.Create(group)
		if err != nil {
			return nil, 0, err
		}
		return group, newEpoch, nil
	}
	return nil, 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}
func (g *localGroupService) DeleteGroup(uuid string, _ uint64) (uint64, error) {
	if g.canWrite {
		g.Logf("I Have READ-WRITE permissions, proceeding with request")

		newEpoch, err := g.groupRepository.SoftDelete(uuid)
		if err != nil {
			return 0, err
		}
		return newEpoch, nil
	}
	return 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}

func (g *localGroupService) GetGroupByUUID(uuid string, _ uint64) (*entity.ChatGroup, uint64, error) {
	g.Logf("Local DB found, proceeding...")
	group, err := g.groupRepository.GetByUUID(uuid)
	if err != nil {
		return nil, 0, fmt.Errorf("Group was not found {%s}", err.Error())
	}
	g.Logf("Found group: %s", group.Name)

	epoch, err := g.globalRepository.GetCurrentEpoch()
	if err != nil {
		epoch = 0
	}

	return group, epoch, nil
}

func (g *localGroupService) GetGroupMembers(uuid string, lastEpoch uint64) ([]*entity.User, uint64, error) {
	g.Logf("Local DB found, proceeding...")
	users, err := g.groupRepository.GetPartecipants(uuid)
	if err != nil {
		return nil, 0, fmt.Errorf("No users in group %s {%s}", uuid, err.Error())
	}
	g.Logf("Found %d users in group %s", len(users), uuid)

	epoch, err := g.globalRepository.GetCurrentEpoch()
	if err != nil {
		epoch = 0
	}

	return users, epoch, nil
}

func (g *localGroupService) AddGroupUser(uuid, userUuid string, _ uint64) (uint64, error) {
	if g.canWrite {
		g.Logf("I Have READ-WRITE permissions, proceeding with request")

		user, err := g.userRepository.GetByUUID(userUuid)
		if err != nil {
			return 0, err
		}

		newEpoch, err := g.groupRepository.AddUser(uuid, user)
		if err != nil {
			return 0, err
		}
		return newEpoch, nil
	}
	return 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}
func (g *localGroupService) RemoveGroupUser(uuid, userUuid string, _ uint64) (uint64, error) {
	if g.canWrite {
		g.Logf("I Have READ-WRITE permissions, proceeding with request")

		newEpoch, err := g.groupRepository.RemoveUser(uuid, userUuid)
		if err != nil {
			return 0, err
		}
		return newEpoch, nil
	}
	return 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}
