package handler

import (
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/sessions"
)

type msgReqFields struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Content  string `json:"content"`
}

type MessageHandler struct {
	messageService service.MessageService
	cookieStore    *sessions.CookieStore
	renderer       *view.PageRenderer
}

func NewMessageHeader(messageService service.MessageService, cookieStore *sessions.CookieStore, renderer *view.PageRenderer) *MessageHandler {
	return &MessageHandler{
		messageService: messageService,
		cookieStore:    cookieStore,
		renderer:       renderer,
	}
}
