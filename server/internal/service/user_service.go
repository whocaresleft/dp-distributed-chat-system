package service

import (
	"encoding/json"
	"fmt"
	"server/cluster/nlog"
	"server/cluster/node/protocol"
	"server/internal/data"
	"server/internal/entity"
	"server/internal/repository"

	"github.com/google/uuid"
)

// Service used to handle users
type UserService interface {
	GetUserByNameTag(name, tag string, lastEpoch uint64) (*entity.User, uint64, error) // Returns the user entity with the given name and tag
	GetUsersByName(name string, lastEpoch uint64) ([]*entity.User, uint64, error)      // Returns all the users that match the given name.
	DeleteUser(uuid string, lastEpoch uint64) (uint64, error)                          // Deletes the user with the given uuid.
}

// Proxy is the implementation of the service on the input nodes.
// Since they hold no database, they can only forward the request towards a node that can handle it.
type proxyUserService struct {
	forwarder data.Forwarder // Forwards the requests and returns the responses
	logger    nlog.Logger    // Logs a format string
}

func NewProxyUserService(forwarder data.Forwarder, logger nlog.Logger) UserService {
	return &proxyUserService{
		forwarder: forwarder,
		logger:    logger,
	}
}

func (p *proxyUserService) Logf(format string, v ...any) {
	p.logger.Logf(format, v...)
}

func (p *proxyUserService) GetUserByNameTag(name, tag string, lastEpoch uint64) (*entity.User, uint64, error) {
	p.Logf("Forwarding the request")
	payload, err := json.Marshal(map[string]string{
		"username": name,
		"tag":      tag,
	})
	if err != nil {
		return nil, 0, err
	}

	response, err := p.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionUserGetNameTag, lastEpoch, payload)
	if err != nil {
		p.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("User was not found. %v", response.ErrorMessage)
		p.Logf("%v", s.Error())
		return nil, 0, s
	}

	var u entity.User
	if err := json.Unmarshal(response.Payload, &u); err != nil {
		return nil, 0, err
	}

	p.Logf("User retrieved correctly")
	return &u, response.Epoch, nil
}

func (p *proxyUserService) GetUsersByName(name string, lastEpoch uint64) ([]*entity.User, uint64, error) {
	p.Logf("Forwarding the request")
	payload := []byte(name)

	response, err := p.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionUsersGetName, lastEpoch, payload)
	if err != nil {
		p.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("User was not found. %v", response.ErrorMessage)
		p.Logf("%v", s.Error())
		return nil, 0, s
	}

	var u []*entity.User
	if err := json.Unmarshal(response.Payload, &u); err != nil {
		return nil, 0, err
	}

	p.Logf("Users retrieved correctly")
	return u, response.Epoch, nil
}

func (p *proxyUserService) DeleteUser(userUuid string, lastEpoch uint64) (uint64, error) {
	p.Logf("Forwarding the request")
	payload := []byte(userUuid)

	response, err := p.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionUserDelete, lastEpoch, payload)
	if err != nil {
		p.Logf("Received error on request execution {%v}", err)
		return 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("User was not deleted. %v", response.ErrorMessage)
		p.Logf("%v", s.Error())
		return 0, s
	}

	p.Logf("User deleted correctly")
	return response.Epoch, nil
}

// Local service is the implementation of the service on the persistence nodes.
// If the service is WRITE-ENABLED, he can write to the database.
// Otherwise, it can only read from it's replica, only if it's up to date (this endpoins is reachable only if write-enabled).
type localUserService struct {
	canWrite         bool                        // Is this service node write enables?
	logger           nlog.Logger                 // Logs a format string
	userRepository   repository.UserRepository   // Repository for users
	globalRepository repository.GlobalRepository // Repository for a global stsate
}

func NewLocalUserService(canWrite bool, userRepo repository.UserRepository, globalRepo repository.GlobalRepository, logger nlog.Logger) UserService {
	return &localUserService{
		canWrite:         canWrite,
		logger:           logger,
		globalRepository: globalRepo,
		userRepository:   userRepo,
	}
}
func (p *localUserService) Logf(format string, v ...any) {
	p.logger.Logf(format, v...)
}

func (p *localUserService) GetUserByNameTag(name, tag string, _ uint64) (*entity.User, uint64, error) {
	p.Logf("Local DB found, proceeding...")
	u, err := p.userRepository.GetByNameTag(name, tag)
	if err != nil {
		return nil, 0, fmt.Errorf("User was not found {%s}", err.Error())
	}
	p.Logf("Found user: %sv", u)

	epoch, err := p.globalRepository.GetCurrentEpoch()
	if err != nil {
		epoch = 0
	}

	return u, epoch, nil
}

func (p *localUserService) GetUsersByName(name string, _ uint64) ([]*entity.User, uint64, error) {
	p.Logf("Local DB found, proceeding...")
	users, err := p.userRepository.GetByName(name)
	if err != nil {
		return nil, 0, fmt.Errorf("No users with such name {%s}", err.Error())
	}
	p.Logf("Found %d users", len(users))

	epoch, err := p.globalRepository.GetCurrentEpoch()
	if err != nil {
		epoch = 0
	}

	return users, epoch, nil
}

func (p *localUserService) DeleteUser(uuid string, _ uint64) (uint64, error) {
	if p.canWrite {
		p.Logf("I Have READ-WRITE permission, proceeding with request")

		newEpoch, err := p.userRepository.SoftDelete(uuid)
		if err != nil {
			return 0, err
		}
		p.Logf("User deleted outcome {err:%v, newEpoch:%d}", err, newEpoch)
		return newEpoch, nil
	}
	return 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}
