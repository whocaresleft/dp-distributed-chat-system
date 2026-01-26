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
	"golang.org/x/crypto/bcrypt"
)

// Service used for the user registration and login phases
type AuthService interface {
	Register(username, tag, password string) (*entity.User, uint64, error)                // Tries to create a new user in the system, returing it if successful
	Login(username, tag, password string, lastEpoch uint64) (*entity.User, uint64, error) // Tries to authenticate a user via its credentials, returing the user entity if successful.
}

// Proxy is the implementation of the service on the input nodes.
// Since they hold no database, they can only forward the request towards a node that can handle it.
type proxyAuthService struct {
	forwarder data.Forwarder // Forwards the requests and returns the responses
	logger    nlog.Logger    // Logs a format string
}

func NewProxyAuthService(forwarder data.Forwarder, logger nlog.Logger) AuthService {
	return &proxyAuthService{
		forwarder: forwarder,
		logger:    logger,
	}
}

func (p *proxyAuthService) Logf(format string, v ...any) {
	p.logger.Logf(format, v...)
}

func (p *proxyAuthService) Register(username, tag, password string) (*entity.User, uint64, error) {
	p.Logf("Forwarding the registration request")

	payload, err := json.Marshal(map[string]string{
		"username": username,
		"tag":      tag,
		"password": password,
	})
	if err != nil {
		return nil, 0, err
	}

	response, err := p.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionUserRegister, 0, payload)
	if err != nil {
		p.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Registration failed... Try again later. %v", response.ErrorMessage)
		p.Logf("%s", s.Error())
		return nil, 0, s
	}

	var u *entity.User
	json.Unmarshal(response.Payload, u)

	p.Logf("User correctly registered")
	return u, response.Epoch, nil
}

func (p *proxyAuthService) Login(username, tag, password string, lastEpoch uint64) (*entity.User, uint64, error) {
	p.Logf("Forwarding the login request")

	payload, err := json.Marshal(map[string]string{
		"username": username,
		"tag":      tag,
		"password": password,
	})
	if err != nil {
		return nil, 0, err
	}

	response, err := p.forwarder.ExecuteRemote(uuid.New().String(), protocol.ActionUserLogin, lastEpoch, payload)
	if err != nil {
		p.Logf("Received error on request execution {%v}", err)
		return nil, 0, err
	}
	if response.Status != protocol.SUCCESS {
		s := fmt.Errorf("Login failed... Try again later. %v", response.ErrorMessage)
		p.Logf("%v", s.Error())
		return nil, 0, s
	}

	var u entity.User
	if err := json.Unmarshal(response.Payload, &u); err != nil {
		return nil, 0, err
	}

	p.Logf("User correctly logged in")
	return &u, response.Epoch, nil
}

// Local service is the implementation of the service on the persistence nodes.
// If the service is WRITE-ENABLED, he can write to the database.
// Otherwise, it can only read from it's replica, only if it's up to date (this endpoins is reachable only if write-enabled).
type localAuthService struct {
	canWrite         bool                        // Is this service node write enables?
	userRepository   repository.UserRepository   // Repository for users
	globalRepository repository.GlobalRepository // Repository for a global stsate
	logger           nlog.Logger                 // Logs a format string
}

func NewLocalAuthService(canWrite bool, userRepo repository.UserRepository, globalRepo repository.GlobalRepository, logger nlog.Logger) AuthService {
	return &localAuthService{
		canWrite:         canWrite,
		userRepository:   userRepo,
		globalRepository: globalRepo,
		logger:           logger,
	}
}

func (a *localAuthService) Logf(format string, v ...any) {
	a.logger.Logf(format, v...)
}

func (a *localAuthService) Register(username, tag, password string) (*entity.User, uint64, error) {

	if a.canWrite {
		a.Logf("I Have READ-WRITE permission, proceeding with request")

		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			a.Logf("Could not calculate hash{%v}", err)
			return nil, 0, err
		}

		uuid := uuid.New().String()

		u := &entity.User{
			UUID:      uuid,
			Username:  username,
			Tag:       tag,
			CreatedAt: time.Now(),
			Epoch:     0,

			Secret: entity.UserSecret{
				UserUUID: uuid,
				Hash:     string(hash),
			},
		}
		newEpoch, err := a.userRepository.Create(u)
		if err != nil {
			return nil, 0, err
		}
		return u, newEpoch, nil
	}
	return nil, 0, fmt.Errorf("Can not handle the request. BAD CASE SHOULD NOT END UP HERE")
}

func (a *localAuthService) Login(username, tag, password string, _ uint64) (*entity.User, uint64, error) {
	a.Logf("Local DB found, proceeding...")
	u, err := a.userRepository.GetForLogin(username, tag)
	if err != nil {
		return nil, 0, fmt.Errorf("User was not found {%s}", err.Error())
	}

	epoch, err := a.globalRepository.GetCurrentEpoch()
	if err != nil {
		epoch = 0
	}

	userSecret := u.Secret
	if err = bcrypt.CompareHashAndPassword([]byte(userSecret.Hash), []byte(password)); err != nil {
		return nil, 0, fmt.Errorf("Wrong credentials: %v. %v AND %v", err, userSecret.Hash, password)
	}
	return u, epoch, nil
}
