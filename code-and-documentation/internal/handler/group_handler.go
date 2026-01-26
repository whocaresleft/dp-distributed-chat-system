/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package handler

import (
	"encoding/json"
	"net/http"
	"net/url"
	"server/internal/entity"
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

// GroupHandler is used to handle routes regarding groups.
// These include: group creation and deletion as well as members inserting and removal
type GroupHandler struct {
	store        *sessions.CookieStore
	userService  service.UserService
	groupService service.GroupService
	renderer     *view.PageRenderer
}

func NewGroupHandler(groupService service.GroupService, userService service.UserService, store *sessions.CookieStore, renderer *view.PageRenderer) *GroupHandler {
	return &GroupHandler{store, userService, groupService, renderer}
}

// Searches for a specific group
func (g *GroupHandler) GetGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	_, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	lastEpoch := extractUint64(r.Context().Value("epoch"))

	group, newEpoch, err := g.groupService.GetGroupByUUID(uuid, lastEpoch)
	if err != nil {
		http.Error(w, "Group was not found", http.StatusNotFound)
		return
	}

	session, _ := g.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	data := map[string]interface{}{
		"LoggedUser": session.Values["username"],
		"LoggedTag":  session.Values["tag"],
		"Group":      group,
	}

	err = g.renderer.RenderTemplate(w, "group.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Creates a group
func (g *GroupHandler) CreateGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.FormValue("groupname")

	thisUser, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	group, newEpoch, err := g.groupService.CreateGroup(groupName, &thisUser, lastEpoch)
	if err != nil {
		http.Error(w, "The group could not be created.", http.StatusBadRequest)
		return
	}

	session, _ := g.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"group":  group,
		"status": "success",
	})
}

// Deletes a group
func (g *GroupHandler) DeleteGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	user, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if !isUserInGroup(user, uuid, lastEpoch, g.groupService) {
		http.Error(w, "Could not delete group since user is not a member", http.StatusUnauthorized)
		return
	}

	newEpoch, err := g.groupService.DeleteGroup(uuid, lastEpoch)
	if err != nil {
		http.Error(w, "Could not delete group", http.StatusInternalServerError)
		return
	}
	session, _ := g.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

// Retrieves the members of a specific group
func (g *GroupHandler) GetGroupUsers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	user, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if !isUserInGroup(user, uuid, lastEpoch, g.groupService) {
		http.Error(w, "Could not list group members since user is not a member", http.StatusUnauthorized)
		return
	}

	group, _, err := g.groupService.GetGroupByUUID(uuid, lastEpoch)
	if err != nil {
		http.Error(w, "Group does not exist", http.StatusBadRequest)
		return
	}

	users, newEpoch, err := g.groupService.GetGroupMembers(uuid, lastEpoch)
	if err != nil {
		http.Error(w, "Could not gather users", http.StatusBadRequest)
		return
	}

	session, _ := g.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	data := map[string]interface{}{
		"LoggedUser": session.Values["username"],
		"LoggedTag":  session.Values["tag"],
		"Group":      group,
		"Members":    users,
	}

	err = g.renderer.RenderTemplate(w, "group_members.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Adds a user to a group
func (g *GroupHandler) AddGroupUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	username := r.FormValue("username")
	tag := r.FormValue("tag")

	user, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if !isUserInGroup(user, uuid, lastEpoch, g.groupService) {
		http.Error(w, "Could not delete group since user is not a member", http.StatusUnauthorized)
		return
	}

	toAdd, _, err := g.userService.GetUserByNameTag(username, tag, lastEpoch)
	if err != nil {
		http.Error(w, "User does not exist", http.StatusInternalServerError)
		return
	}

	newEpoch, err := g.groupService.AddGroupUser(uuid, toAdd.UUID, lastEpoch)
	if err != nil {
		http.Error(w, "Group does not exist", http.StatusBadRequest)
		return
	}

	session, _ := g.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	http.Redirect(w, r, "/groups/"+url.PathEscape(uuid)+"/members", http.StatusSeeOther)
}

// Removes a user from a group
func (g *GroupHandler) RemoveGroupUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupUuid := vars["group-uuid"]
	userUuid := vars["user-uuid"]

	user, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if !isUserInGroup(user, groupUuid, lastEpoch, g.groupService) {
		http.Error(w, "User is not in group, cannot remove other members", http.StatusUnauthorized)
		return
	}

	newEpoch, err := g.groupService.RemoveGroupUser(groupUuid, userUuid, lastEpoch)
	if err != nil {
		http.Error(w, "User could not be removed", http.StatusInternalServerError)
		return
	}

	session, _ := g.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

// Shows a page with a form to create a group
func (g *GroupHandler) CreateGroupForm(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(entity.User)

	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	data := map[string]interface{}{
		"LoggedUser": user.Username,
		"LoggedTag":  user.Tag,
	}

	err := g.renderer.RenderTemplate(w, "create_group.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
