package handler

import (
	"net/http"
	"server/internal/entity"
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

type UserHandler struct {
	store       *sessions.CookieStore
	userService service.UserService
	renderer    *view.PageRenderer
}

func NewUserHandler(userService service.UserService, store *sessions.CookieStore, renderer *view.PageRenderer) *UserHandler {
	return &UserHandler{store, userService, renderer}
}

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

	lastEpoch := extractEpoch(r.Context().Value("epoch"))

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

func (u *UserHandler) GetUsersByName(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["username"]

	_, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractEpoch(r.Context().Value("epoch"))

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

	lastEpoch := extractEpoch(r.Context().Value("epoch"))

	_, err := u.userService.DeleteUser(uuid, lastEpoch)
	if err != nil {
		http.Error(w, "No users were found with such uuid. Or it could not be deleted", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func extractEpoch(v any) uint64 {
	var x uint64
	if val, ok := v.(uint64); ok {
		x = val
	} else if val, ok := v.(int); ok {
		x = uint64(val)
	} else {
		x = 0
	}
	return x
}
