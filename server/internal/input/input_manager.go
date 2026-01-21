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

	authService service.AuthService
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
	return i.logger != nil && i.authService != nil
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

	cookieStore := sessions.NewCookieStore([]byte(cfg.SecretKey))
	cookieStore.Options = &sessions.Options{
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
	renderer := view.NewPageRenderer(templates)

	// Handlers
	authHandler := handler.NewAuthHandler(i.authService, cookieStore, renderer)

	// Router
	r := mux.NewRouter()

	// Authentication routes
	r.HandleFunc("/register", authHandler.Register).Methods("POST", "GET")
	r.HandleFunc("/login", authHandler.Login).Methods("POST", "GET")
	r.HandleFunc("/logout", authHandler.Logout).Methods("GET")

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
	close(i.stopFromOutsideChan)
	<-i.doneFromInsideChan
	i.running.Store(false)
}
