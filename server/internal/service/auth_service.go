package service

import (
	"encoding/json"
	"fmt"
	"server/cluster/nlog"
	"server/internal/data"
	"server/internal/entity"
	repository "server/internal/repository"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type AuthService interface {
	Register(username, tag, password string) error
	Login(username, tag, password string) (*entity.User, error)
}

type authService struct {
	forwarder      data.Forwarder
	userRepository repository.UserRepository
	logger         nlog.Logger
}

func NewAuthService(userRepo repository.UserRepository, forwarder data.Forwarder, logger nlog.Logger) AuthService {
	return &authService{
		userRepository: userRepo,
		forwarder:      forwarder,
		logger:         logger,
	}
}

func (a *authService) Logf(format string, v ...any) {
	a.logger.Logf(format, v...)
}

func (a *authService) Register(username, tag, password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		a.Logf("Could not calculate hash{%v}", err)
		return err
	}

	if a.userRepository != nil { // We can use local DB
		a.Logf("Local DB found, proceeding...")

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
			Sessions: make([]entity.UserSession, 0),
		}
		err := a.userRepository.Create(u)
		a.Logf("User creation outcome {%v}", err)
		return err
	}
	a.Logf("No local DB found, forwarding the request")

	payload, err := json.Marshal(AuthRequest{"register", username, tag, string(hash)})
	if err != nil {
		return err
	}

	// No local DB, forward request
	response, err := a.ExecuteRemote(payload)
	if err != nil {
		a.Logf("Received error on request execution {%v}", err)
		return err
	}
	if response.Status != data.SUCCESS {
		s := fmt.Errorf("Registration failed... Try again later. %v", response.ErrorMessage)
		a.Logf("%s", s.Error())
		return s
	}
	a.Logf("User correctly registered")
	return nil
}

func (a *authService) Login(username, tag, password string) (*entity.User, error) {
	if a.userRepository != nil {
		a.Logf("Local DB found, proceeding...")
		u, err := a.userRepository.GetByNameTag(username, tag)
		if err != nil {
			return nil, fmt.Errorf("User was not found {%s}", err.Error())
		}

		userSecret := u.Secret
		if err = bcrypt.CompareHashAndPassword([]byte(userSecret.Hash), []byte(password)); err != nil {
			return nil, fmt.Errorf("Wrong credentials")
		}
		return u, nil
	}

	payload, err := json.Marshal(AuthRequest{"login", username, tag, password})
	if err != nil {
		return nil, err
	}
	a.Logf("No local DB found, forwarding the request")

	// No local DB, forward request
	response, err := a.ExecuteRemote(payload)
	if err != nil {
		a.Logf("Received error on request execution {%v}", err)
		return nil, err
	}
	if response.Status != data.SUCCESS {
		s := fmt.Errorf("Login failed... Try again later. %v", response.ErrorMessage)
		a.Logf("%v", s.Error())
		return nil, s
	}

	var u entity.User
	if err := json.Unmarshal(response.Payload, &u); err != nil {
		return nil, err
	}

	a.Logf("User correctly logged in")
	return &u, nil
}

func (a *authService) ExecuteRemote(payload []byte) (*data.DataMessage, error) {
	messageId := uuid.New().String()
	return a.forwarder.ExecuteRemote(messageId, payload)
}

type AuthRequest struct {
	Action   string `json:"action"`
	User     string `json:"user"`
	Tag      string `json:"tag"`
	Password string `json:"hash"`
}
