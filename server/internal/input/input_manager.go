package input

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"server/cluster/nlog"
	"server/internal"
	"server/internal/handler"
	"server/internal/middleware"
	"server/internal/service"
	"server/internal/view"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

type IptConfig struct {
	ServerPort        uint16
	ReadTimeout       int64
	WriteTimeout      int64
	TemplateDirectory string
	SecretKey         string
}

type InputManager struct { // Manages HTTP input on the input nodes
	running atomic.Bool
	paused  atomic.Bool

	logger nlog.Logger
	server *http.Server

	stopFromOutsideChan chan struct{}
	doneFromInsideChan  chan struct{}

	serverPort         string
	templatesDirectory string
	readTimeout        int64
	writeTimeout       int64

	renderer *view.PageRenderer
	store    *sessions.CookieStore

	authService service.AuthService
	userService service.UserService
}

func NewInputManager() *InputManager {
	return &InputManager{
		running:             atomic.Bool{},
		paused:              atomic.Bool{},
		stopFromOutsideChan: make(chan struct{}),
		doneFromInsideChan:  make(chan struct{}),
	}
}

func (i *InputManager) IsReady() bool {
	return i.logger != nil && i.authService != nil && !i.IsRunning() && i.userService != nil
}

func (i *InputManager) IsRunning() bool {
	return i.running.Load()
}

func (i *InputManager) SetLogger(l nlog.Logger) {
	i.logger = l
}

func (i *InputManager) SetAuthService(as service.AuthService) {
	i.authService = as
}

func (i *InputManager) SetUserService(us service.UserService) {
	i.userService = us
}

func (i *InputManager) GetAuthService() service.AuthService {
	return i.authService
}

func (i *InputManager) Logf(format string, a ...any) {
	i.logger.Logf(format, a...)
}

func (i *InputManager) SetPause(paused bool) {
	i.paused.Store(paused)
}

func (i *InputManager) IsPaused() bool {
	return i.paused.Load()
}

func (i *InputManager) Run(ctx context.Context, cfg *IptConfig) error {
	i.Logf("Input service started...")

	if !i.IsReady() {
		return fmt.Errorf("The Input manager is not ready... Missing components")
	}

	exePath, err := os.Executable()
	if err != nil {
		return err
	}
	exeDir := filepath.Dir(exePath)

	i.store = sessions.NewCookieStore([]byte(cfg.SecretKey))
	i.store.Options = &sessions.Options{
		Path:     "/",
		HttpOnly: true,
		Secure:   false,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(7 * 24 * time.Hour.Seconds()),
	}

	// Load templates and page renderer
	templates, err := internal.RetrieveWebTemplates(filepath.Join(exeDir, cfg.TemplateDirectory))
	if err != nil {
		return err
	}
	i.renderer = view.NewPageRenderer(templates)

	// Handlers
	authHandler := handler.NewAuthHandler(i.authService, i.store, i.renderer)
	userHandler := handler.NewUserHandler(i.userService, i.store, i.renderer)

	// Router
	r := mux.NewRouter()

	// Main page
	r.HandleFunc("/", i.HomeHandler).Methods("GET")

	// Authentication routes
	r.HandleFunc("/register", authHandler.Register).Methods("POST", "GET")
	r.HandleFunc("/login", authHandler.Login).Methods("POST", "GET")
	r.HandleFunc("/logout", authHandler.Logout).Methods("GET")

	// User routes
	//r.HandleFunc("/users/{username}/{tag:[0-9]{3,6}}/chat", userHandler.GetMessages).Methods("GET")
	//r.HandleFunc("/users/{username}/{tag:[0-9]{3,6}}/chat", userHandler.SendMessage).Methods("POST")
	r.HandleFunc("/users/{username}/{tag:[0-9]{3,6}}", middleware.AuthMiddleware(i.store, userHandler.GetUser)).Methods("GET")
	r.HandleFunc("/users/{username}", middleware.AuthMiddleware(i.store, userHandler.GetUsersByName)).Methods("GET")
	r.HandleFunc("/users/{uuid}", middleware.AuthMiddleware(i.store, userHandler.DeleteUser)).Methods("DELETE")
	r.HandleFunc("/users", middleware.AuthMiddleware(i.store, i.SearchHandler)).Methods("GET")

	r.Use(i.PauseMiddleware)

	i.server = &http.Server{
		Addr:           fmt.Sprintf(":%d", cfg.ServerPort),
		Handler:        r,
		ReadTimeout:    time.Duration(cfg.ReadTimeout * int64(time.Second)),
		WriteTimeout:   time.Duration(cfg.WriteTimeout * int64(time.Second)),
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		select {
		case <-ctx.Done():
			i.Logf("Received stop signal. Shutting down...")
		case <-i.stopFromOutsideChan:
			i.Logf("Server was asked to stop. Shutting down...")
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := i.server.Shutdown(shutdownCtx); err != nil {
			i.Logf("Error during shutdown... %v\n", err)
		}
		close(i.doneFromInsideChan)
	}()

	if err := i.server.ListenAndServe(); err != http.ErrServerClosed {
		i.Logf("FATAL: HTTP Server error{%v}\n", err)
		return err
	}

	i.Logf("Http server started on port {%d}", cfg.ServerPort)

	i.running.Store(true)

	return nil
}

func (i *InputManager) Stop() {
	i.Logf("Calling close")
	close(i.stopFromOutsideChan)
	i.Logf("Sending to internal chan")
	<-i.doneFromInsideChan
	i.Logf("Storing run = false")
	i.running.Store(false)
}

func (i *InputManager) PauseMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if i.paused.Load() {
			w.Header().Set("Retry-After", "5")
			http.Error(w, "Maintainence in progress", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (i *InputManager) HomeHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := i.store.Get(r, "auth-session")

	username, okU := session.Values["username"].(string)
	tag, okT := session.Values["tag"].(string)

	data := map[string]interface{}{
		"LoggedUser": "",
		"LoggedTag":  "",
	}

	if okU && okT {
		data["LoggedUser"] = username
		data["LoggedTag"] = tag
	}

	i.renderer.RenderTemplate(w, "index.html", data)
}

func (i *InputManager) SearchHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := i.store.Get(r, "auth-session")

	username, okU := session.Values["username"].(string)
	tag, okT := session.Values["tag"].(string)

	data := map[string]interface{}{
		"LoggedUser": "",
		"LoggedTag":  "",
	}

	if okU && okT {
		data["LoggedUser"] = username
		data["LoggedTag"] = tag
	}

	i.renderer.RenderTemplate(w, "search_user.html", data)
}
