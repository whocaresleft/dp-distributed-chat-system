package handler

import (
	"net/http"
	"server/internal/service"
	"server/internal/view"

	"github.com/gorilla/sessions"
)

type reqFormFields struct {
	Username string `json:"username"`
	Tag      string `json:"tag"`
	Password string `json:"password"`
}

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

func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if err := h.renderer.RenderTemplate(w, "register.html", nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	var request = reqFormFields{}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Error occurred while parsing the form", http.StatusBadRequest)
		return
	}

	request.Username = r.FormValue("username")
	request.Tag = r.FormValue("tag")
	request.Password = r.FormValue("password")

	if err := h.authService.Register(request.Username, request.Tag, request.Password); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if err := h.renderer.RenderTemplate(w, "login.html", nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	var request = reqFormFields{}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Error parsing form", http.StatusBadRequest)
		return
	}

	request.Username = r.FormValue("username")
	request.Tag = r.FormValue("tag")
	request.Password = r.FormValue("password")

	user, err := h.authService.Login(request.Username, request.Tag, request.Password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	session, _ := h.cookieStore.Get(r, "auth-session")
	session.Values["user_id"] = user.UUID
	session.Values["username"] = user.Username
	session.Values["tag"] = user.Tag
	if err := sessions.Save(r, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	session, _ := h.cookieStore.Get(r, "auth-session")
	session.Options.MaxAge = -1
	if err := sessions.Save(r, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
