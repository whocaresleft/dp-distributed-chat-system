/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package handler

import (
	"net/http"
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/sessions"
)

type authReqFields struct {
	Username string `json:"username"`
	Tag      string `json:"tag"`
	Password string `json:"password"`
}

// AuthHandler helps in managing user registration and authentication
type AuthHandler struct {
	authService service.AuthService
	cookieStore *sessions.CookieStore
	renderer    *view.PageRenderer
}

func NewAuthHandler(authService service.AuthService, cookieStore *sessions.CookieStore, renderer *view.PageRenderer) *AuthHandler {
	return &AuthHandler{
		authService: authService,
		cookieStore: cookieStore,
		renderer:    renderer,
	}
}

// Registers a user
// If the method is GET, a registration form is shown
// If it's POST, it retrieve the input fields and uses the auth server to register the user
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if err := h.renderer.RenderTemplate(w, "register.html", nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	var request = authReqFields{}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Error occurred while parsing the form", http.StatusBadRequest)
		return
	}

	request.Username = r.FormValue("username")
	request.Tag = r.FormValue("tag")
	request.Password = r.FormValue("password")

	if !validateTag(request.Tag) {
		http.Error(w, "The tag is not valid, it must be a sequence of 3 to 6 numbers", http.StatusInternalServerError)
		return
	}

	_, newEpoch, err := h.authService.Register(request.Username, request.Tag, request.Password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	session, _ := h.cookieStore.Get(r, "auth-session")
	session.Values["last-seen-epoch"] = newEpoch
	if err := sessions.Save(r, w); err != nil {
		http.Error(w, "Saving cookie", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

// Login handles the authentication phase
// If this function got called a GET request, it shows the login form
// Otherwise, for POST, it retrieves the form's input fields and tries to authenticate the user using the auth service
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if err := h.renderer.RenderTemplate(w, "login.html", nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	var request = authReqFields{}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Error parsing form", http.StatusBadRequest)
		return
	}

	request.Username = r.FormValue("username")
	request.Tag = r.FormValue("tag")
	request.Password = r.FormValue("password")

	if !validateTag(request.Tag) {
		http.Error(w, "The tag is not valid, it must be a sequence of 3 to 6 numbers", http.StatusInternalServerError)
		return
	}

	var lastEpoch uint64 = 0
	if session, err := h.cookieStore.Get(r, "auth-session"); err == nil {
		if val, exists := session.Values["last-seen-epoch"]; exists {
			lastEpoch = val.(uint64)
		}
	}

	user, newEpoch, err := h.authService.Login(request.Username, request.Tag, request.Password, lastEpoch)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	session, _ := h.cookieStore.Get(r, "auth-session")
	session.Values["user_uuid"] = user.UUID
	session.Values["username"] = user.Username
	session.Values["tag"] = user.Tag
	session.Values["last-seen-epoch"] = newEpoch
	if err := sessions.Save(r, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

// Logout deletes the the current user's session, effectively logging him out
func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	session, _ := h.cookieStore.Get(r, "auth-session")
	session.Options.MaxAge = -1
	if err := sessions.Save(r, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
