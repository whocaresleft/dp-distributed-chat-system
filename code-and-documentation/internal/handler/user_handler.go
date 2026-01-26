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
	"server/internal/entity"
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

// UserHandler is used for all the routes regarding user (apart from authentication)
// It is used to search for users and delete users
type UserHandler struct {
	store       *sessions.CookieStore
	userService service.UserService
	renderer    *view.PageRenderer
}

func NewUserHandler(userService service.UserService, store *sessions.CookieStore, renderer *view.PageRenderer) *UserHandler {
	return &UserHandler{store, userService, renderer}
}

// Searches for a particular user
func (u *UserHandler) GetUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["username"]
	tag := vars["tag"]

	if !validateTag(tag) {
		http.Error(w, "Tag is wrong, it must be a sequence of 3-6 numbers", http.StatusBadRequest)
		return
	}

	_, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	lastEpoch := extractUint64(r.Context().Value("epoch"))

	user, newEpoch, err := u.userService.GetUserByNameTag(name, tag, lastEpoch)
	if err != nil {
		http.Error(w, "User was not found...", http.StatusNotFound)
		return
	}

	session, _ := u.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	data := map[string]interface{}{
		"LoggedUser":   session.Values["username"],
		"LoggedTag":    session.Values["tag"],
		"ProfiledUser": user,
	}

	err = u.renderer.RenderTemplate(w, "user.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Search for all users with a given name
func (u *UserHandler) GetUsersByName(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["username"]

	_, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	users, newEpoch, err := u.userService.GetUsersByName(name, lastEpoch)
	if err != nil {
		http.Error(w, "No users were found with such name...", http.StatusNotFound)
		return
	}

	session, _ := u.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	data := map[string]interface{}{
		"LoggedUser":  session.Values["username"],
		"LoggedTag":   session.Values["tag"],
		"UsersByName": users,
	}

	err = u.renderer.RenderTemplate(w, "users.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Handles the user deletion request
func (u *UserHandler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	user, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if user.UUID != uuid {
		http.Error(w, "Are you trying to delete someone else's account? Nuh-huh", http.StatusUnauthorized)
		return
	}

	lastEpoch := extractUint64(r.Context().Value("epoch"))

	_, err := u.userService.DeleteUser(uuid, lastEpoch)
	if err != nil {
		http.Error(w, "No users were found with such uuid. Or it could not be deleted", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
