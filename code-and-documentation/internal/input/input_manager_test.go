/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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
