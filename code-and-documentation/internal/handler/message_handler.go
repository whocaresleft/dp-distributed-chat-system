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
	"fmt"
	"net/http"
	"net/url"
	"server/internal/entity"
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

type msgReqFields struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Content  string `json:"content"`
}

// MessageHandler is used to handle all message-related routes
// Both private chat and group messages
type MessageHandler struct {
	messageService service.MessageService
	userService    service.UserService
	groupService   service.GroupService
	store          *sessions.CookieStore
	renderer       *view.PageRenderer
}

func NewMessageHandler(messageService service.MessageService, userService service.UserService, groupService service.GroupService, cookieStore *sessions.CookieStore, renderer *view.PageRenderer) *MessageHandler {
	return &MessageHandler{
		userService:    userService,
		messageService: messageService,
		groupService:   groupService,
		store:          cookieStore,
		renderer:       renderer,
	}
}

// Used to send a message in a private chat
func (m *MessageHandler) CreateDMMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := url.PathEscape(vars["username"])
	tag := url.PathEscape(vars["tag"])
	content := r.FormValue("content")

	if !validateTag(tag) {
		http.Error(w, "Tag is wrong, it must be a sequence of 3-6 numbers", http.StatusBadRequest)
		return
	}

	thisUser, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if thisUser.Username == name && thisUser.Tag == tag {
		path := fmt.Sprintf("/users/%s/%s", name, tag)
		http.Redirect(w, r, path, http.StatusSeeOther)
		return
	}

	otherUser, _, err := m.userService.GetUserByNameTag(name, tag, lastEpoch)
	if err != nil {
		http.Error(w, "The requested user does not exist.", http.StatusBadRequest)
		return
	}

	message, newEpoch, err := m.messageService.CreateDMMessage(thisUser.UUID, otherUser.UUID, content, lastEpoch)
	if err != nil {
		http.Error(w, "No users were found with such name...", http.StatusNotFound)
		return
	}

	session, _ := m.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": message,
		"status":  "success",
		"other":   otherUser,
		"myname":  thisUser.Username,
		"mytag":   thisUser.Tag,
	})
}

// Used to send a message to a group
func (m *MessageHandler) CreateGroupMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupUuid := url.PathEscape(vars["uuid"])
	content := r.FormValue("content")

	thisUser, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if !isUserInGroup(thisUser, groupUuid, lastEpoch, m.groupService) {
		http.Error(w, "Cannot access chat, user is not in group", http.StatusUnauthorized)
		return
	}

	message, newEpoch, err := m.messageService.CreateGroupMessage(thisUser.UUID, groupUuid, content, lastEpoch)
	if err != nil {
		http.Error(w, "Could not send message to group...", http.StatusInternalServerError)
		return
	}

	session, _ := m.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": message,
		"status":  "success",
		"myname":  thisUser.Username,
		"mytag":   thisUser.Tag,
		"myuuid":  thisUser.UUID,
	})
}

// Retrieves the messages in a private chat
func (m *MessageHandler) GetDMMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := url.PathEscape(vars["username"])
	tag := url.PathEscape(vars["tag"])

	if !validateTag(tag) {
		http.Error(w, "Tag is wrong, it must be a sequence of 3-6 numbers", http.StatusBadRequest)
		return
	}

	thisUser, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if thisUser.Username == name && thisUser.Tag == tag {
		path := fmt.Sprintf("/users/%s/%s", name, tag)
		http.Redirect(w, r, path, http.StatusSeeOther)
		return
	}

	otherUser, _, err := m.userService.GetUserByNameTag(name, tag, lastEpoch)
	if err != nil {
		http.Error(w, "The requested user does not exist.", http.StatusBadRequest)
		return
	}

	messages, newEpoch, err := m.messageService.GetDM(thisUser.UUID, otherUser.UUID, lastEpoch)
	if err != nil {
		http.Error(w, "No messages found for this chat...", http.StatusNotFound)
		return
	}

	session, _ := m.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	data := map[string]interface{}{
		"LoggedUser": session.Values["username"],
		"LoggedTag":  session.Values["tag"],
		"OtherUser":  otherUser,
		"Messages":   messages,
	}

	err = m.renderer.RenderTemplate(w, "chat.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Retrieves the messages in a group chat
func (m *MessageHandler) GetGroupMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupUuid := url.PathEscape(vars["uuid"])

	thisUser, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	lastEpoch := extractUint64(r.Context().Value("epoch"))

	if !isUserInGroup(thisUser, groupUuid, lastEpoch, m.groupService) {
		http.Error(w, "Cannot access chat, user is not in group", http.StatusUnauthorized)
		return
	}

	group, _, err := m.groupService.GetGroupByUUID(groupUuid, lastEpoch)
	if err != nil {
		http.Error(w, "The group does not exist", http.StatusNotFound)
		return
	}

	partecipants, _, err := m.groupService.GetGroupMembers(groupUuid, lastEpoch)
	if err != nil {
		http.Error(w, "The group does not exist or it does not contain members", http.StatusNotFound)
		return
	}
	partecipantsMap := make(map[string]struct {
		User string `json:"Username"`
		Tag  string `json:"Tag"`
	})
	for _, partecipant := range partecipants {
		partecipantsMap[partecipant.UUID] = struct {
			User string `json:"Username"`
			Tag  string `json:"Tag"`
		}{
			partecipant.Username,
			partecipant.Tag,
		}
	}

	messages, newEpoch, err := m.messageService.GetGroup(groupUuid, lastEpoch)
	if err != nil {
		http.Error(w, "No messages found for this chat...", http.StatusNotFound)
		return
	}

	session, _ := m.store.Get(r, "auth-session")
	if newEpoch > lastEpoch {
		session.Values["last-seen-epoch"] = newEpoch
		sessions.Save(r, w)
	}

	data := map[string]interface{}{
		"LoggedUserUUID": thisUser.UUID,
		"LoggedUser":     thisUser.Username,
		"LoggedTag":      thisUser.Tag,
		"Group":          group,
		"Messages":       messages,
		"Partecipants":   partecipantsMap,
	}

	err = m.renderer.RenderTemplate(w, "group_chat.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
