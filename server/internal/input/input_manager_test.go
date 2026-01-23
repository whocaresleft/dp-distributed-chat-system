package input

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"server/internal/entity"
	"testing"
)

type MockLogger struct{}

func (m *MockLogger) Logf(format string, v ...any) {
	fmt.Printf(format+"\n", v)
}

type MockAuthService struct{}

func (a *MockAuthService) Register(string, string, string) (uint64, error) { return 0, nil }
func (a *MockAuthService) Login(string, string, string, uint64) (*entity.User, uint64, error) {
	return &entity.User{
		UUID:     "a",
		Username: "fr",
		Tag:      "ec",
	}, 0, nil
}

func TestPauseMiddlewareOn(t *testing.T) {
	i := NewInputManager()

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Called despite being paused!")
	})

	toTest := i.PauseMiddleware(nextHandler)

	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()

	i.SetPause(true)

	toTest.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rr.Code)
	}
}

func TestPauseMiddlewareOff(t *testing.T) {
	i := NewInputManager()

	var x int = 10
	y := &x

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*y = 4
	})

	toTest := i.PauseMiddleware(nextHandler)

	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()

	toTest.ServeHTTP(rr, req)

	if rr.Code == http.StatusServiceUnavailable {
		t.Errorf("Got 503, expected 200")
	}

	switch x {
	case 10:
		t.Errorf("Pause middleware was executed despite not being paused")
	case 4:
		// Ok
	default:
		t.Errorf("This case should not even be possible")
	}
}

type MockUserRepo struct{}

func (m *MockUserRepo) Create(user *entity.User) error                                   { return nil }
func (m *MockUserRepo) SoftDelete(uuid string) error                                     { return nil }
func (m *MockUserRepo) UpdateUsername(uuid string, username string, newEpoch uint) error { return nil }
func (m *MockUserRepo) UpdateTag(uuid string, tag string, newEpoch uint) error           { return nil }
func (m *MockUserRepo) UpdatePassword(uuid string, hash string, newEpoch uint) error     { return nil }
func (m *MockUserRepo) GetByUUID(uuid string) (*entity.User, uint64, error) {
	return &entity.User{}, 0, nil
}
func (m *MockUserRepo) GetByNameTag(name, tag string) (*entity.User, uint64, error) {
	return &entity.User{}, 0, nil
}
