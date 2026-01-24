package input

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"server/cluster/nlog"
	"server/internal"
	"server/internal/entity"
	"server/internal/handler"
	"server/internal/middleware"
	"server/internal/service"
	"server/internal/view"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

// Configuration that an InputManager can use to configure itself.
type IptConfig struct {
	ServerPort        string
	ReadTimeout       int64
	WriteTimeout      int64
	TemplateDirectory string
	SecretKey         string
}

// An InputManager handles client interactions via an HTTP server, allowing it to interact with the chat server
type InputManager struct {
	running atomic.Bool // Is the server running or closed?
	paused  atomic.Bool // Is the manager paused? (Used to hot-swap the injected components)

	logger nlog.Logger  // Logs format strings
	server *http.Server // Actual HTTP server hosted by this manager

	stopFromOutsideChan chan struct{} // Channel used to receive stop signals from outside (i.e. calling .Stop())
	doneFromInsideChan  chan struct{} // Channel used to notify an internal goroutine of a stop (it happens after .Stop())

	serverPort         string // Which port is the HTTP server on
	templatesDirectory string // Directory where to look for templates (used to call Config's RetrieveWebTemplates())
	readTimeout        int64
	writeTimeout       int64

	renderer *view.PageRenderer    // Renders web page templates
	store    *sessions.CookieStore // Cookie store for (Gorillamux's) sessions

	authService    service.AuthService
	userService    service.UserService
	messageService service.MessageService
	groupService   service.GroupService

	authHandler    *handler.AuthHandler
	userHandler    *handler.UserHandler
	messageHandler *handler.MessageHandler
	groupHandler   *handler.GroupHandler
}

func NewInputManager() *InputManager {
	return &InputManager{
		running:             atomic.Bool{},
		paused:              atomic.Bool{},
		stopFromOutsideChan: make(chan struct{}),
		doneFromInsideChan:  make(chan struct{}),
	}
}

// Is the manager ready to start? That is, are all the injectable components set?
func (i *InputManager) IsReady() bool {
	return i.logger != nil && i.authService != nil && !i.IsRunning() && i.userService != nil && i.messageService != nil && i.groupService != nil
}

// Tells whether the HTTP server is running or not
func (i *InputManager) IsRunning() bool {
	return i.running.Load()
}

// Tells whether the HTTP server is paused or not
func (i *InputManager) IsPaused() bool {
	return i.paused.Load()
}

// Pauses and unpausess the http server
func (i *InputManager) SetPause(paused bool) {
	i.paused.Store(paused)
}

// Injects a logger
func (i *InputManager) SetLogger(l nlog.Logger) {
	i.logger = l
}

// Logs a formatted string, using the logger component
func (i *InputManager) Logf(format string, a ...any) {
	i.logger.Logf(format, a...)
}

// Injects an authentication service
func (i *InputManager) SetAuthService(as service.AuthService) {
	i.authService = as
}

// Injects a user service
func (i *InputManager) SetUserService(us service.UserService) {
	i.userService = us
}

// Injects a message service
func (i *InputManager) SetMessageService(ms service.MessageService) {
	i.messageService = ms
}

// Injects a group service
func (i *InputManager) SetGroupService(gs service.GroupService) {
	i.groupService = gs
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
		Secure:   false, // To be frank, for simplicity
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
	i.authHandler = handler.NewAuthHandler(i.authService, i.store, i.renderer)
	i.userHandler = handler.NewUserHandler(i.userService, i.store, i.renderer)
	i.messageHandler = handler.NewMessageHandler(i.messageService, i.userService, i.groupService, i.store, i.renderer)
	i.groupHandler = handler.NewGroupHandler(i.groupService, i.userService, i.store, i.renderer)

	// Router
	r := mux.NewRouter()

	//==============================================================================//
	// An indirect approach is taken for the routes, because dynamic lookup for the //
	// handler was necessary, as it could happen that the handler changes runtime   //
	//==============================================================================//

	// Main page
	r.HandleFunc("/", i.HomeHandler).Methods("GET")

	// Authentication routes
	r.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) { i.authHandler.Register(w, r) }).Methods("POST", "GET")
	r.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) { i.authHandler.Login(w, r) }).Methods("POST", "GET")
	r.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) { i.authHandler.Logout(w, r) }).Methods("GET")

	// User routes
	r.HandleFunc("/users/{username}/{tag:[0-9]{3,6}}/chat", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.messageHandler.GetDMMessages)
	}).Methods("GET")

	r.HandleFunc("/users/{username}/{tag:[0-9]{3,6}}/chat", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.messageHandler.CreateDMMessage)
	}).Methods("POST")

	r.HandleFunc("/users/{username}/{tag:[0-9]{3,6}}", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.userHandler.GetUser)
	}).Methods("GET")

	r.HandleFunc("/users/{username}", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.userHandler.GetUsersByName)
	}).Methods("GET")

	r.HandleFunc("/users/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.userHandler.DeleteUser)
	}).Methods("DELETE")

	r.HandleFunc("/users", middleware.AuthMiddleware(i.store, i.SearchHandler)).Methods("GET")

	// Group routes
	r.HandleFunc("/groups/{uuid}/chat", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.messageHandler.GetGroupMessages)
	}).Methods("GET")

	r.HandleFunc("/groups/{uuid}/chat", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.messageHandler.CreateGroupMessage)
	}).Methods("POST")

	r.HandleFunc("/groups/{uuid}/members", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.GetGroupUsers)
	}).Methods("GET")

	r.HandleFunc("/groups/{uuid}/members", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.AddGroupUser)
	}).Methods("POST")

	r.HandleFunc("/groups/{group-uuid}/members/{user-uuid}", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.RemoveGroupUser)
	}).Methods("DELETE")

	r.HandleFunc("/groups/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.GetGroup)
	}).Methods("GET")

	r.HandleFunc("/groups/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.DeleteGroup)
	}).Methods("DELETE")

	r.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.CreateGroupForm)
	}).Methods("GET")

	r.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		middleware.AuthMiddleware(i.store, i.groupHandler.CreateGroup)
	}).Methods("POST")

	// Applies the pause middleware on each route (check down for information)
	r.Use(i.PauseMiddleware)

	i.server = &http.Server{
		Addr:           ":" + cfg.ServerPort,
		Handler:        r,
		ReadTimeout:    time.Duration(cfg.ReadTimeout * int64(time.Second)),
		WriteTimeout:   time.Duration(cfg.WriteTimeout * int64(time.Second)),
		MaxHeaderBytes: 1 << 20,
	}

	go func() {

		// Waits for any signal on one of the two channels. <-ctx.Done() is received when the caller, that passed ctx, finishes, or if the corresponding cancel() function is called.
		// <-i.stopFromOutsideChan is received when i.Stop() is called.
		select {
		case <-ctx.Done():
			i.Logf("Received stop signal. Shutting down...")
		case <-i.stopFromOutsideChan:
			i.Logf("Server was asked to stop. Shutting down...")
		}

		// In each case, the server is shut off. This is done gracefully, by
		// giving a 10 second period for all active connections to finish before being turned off.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := i.server.Shutdown(shutdownCtx); err != nil {
			i.Logf("Error during shutdown... %v\n", err)
		}

		// Notifies the inside chan (check down)
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

// Stops the manager
func (i *InputManager) Stop() {
	i.Logf("Calling close")
	close(i.stopFromOutsideChan) // First the channel 'from outside' is closed, sending a signal to the goroutine above.
	i.Logf("Sending to internal chan")
	<-i.doneFromInsideChan // We wait for the goroutine to finish, before storing running = false. Since the 'inside channel' is closed, this instruction unblocks and the server is set to off logically.
	i.Logf("Storing run = false")
	i.running.Store(false)
}

// Hot swaps the actively used services of the manager.
// It is required that the manager is paused, otherwise an error is returned.
// The correct workflow should be SetPause(true) => ... Setting new services ... => HotSwap() => SetPause(false)
// Of course, this requires that the services are changed using the injecting 'Set' functions
func (i *InputManager) HotSwap() error {
	if !i.IsPaused() {
		return fmt.Errorf("We cannot proceed while Unpaused, it's too risky.")
	}

	// Update the handlers with the new services
	i.authHandler = handler.NewAuthHandler(i.authService, i.store, i.renderer)
	i.userHandler = handler.NewUserHandler(i.userService, i.store, i.renderer)
	i.messageHandler = handler.NewMessageHandler(i.messageService, i.userService, i.groupService, i.store, i.renderer)
	i.groupHandler = handler.NewGroupHandler(i.groupService, i.userService, i.store, i.renderer)
	return nil
}

// Middleware used to pause incoming HTTP requests without turning the server off.
func (i *InputManager) PauseMiddleware(next http.Handler) http.Handler {

	// Here the paused bool is used, to make sure that, if necessary, no HTTP request is server.
	// This is much useful since it allows us to hot swap services without stopping and reopening the server.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if i.paused.Load() {
			w.Header().Set("Retry-After", "5")
			http.Error(w, "Maintainence in progress", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Simple handler that retrieves the user name and tag from the session and renders an empty main page.
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

// Simple handler that retrieves the user name and tag from the session and renders a page with a search bar, to search for other users.
func (i *InputManager) SearchHandler(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(entity.User)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	data := map[string]string{
		"LoggedUser": user.Username,
		"LoggedTag":  user.Tag,
	}

	i.renderer.RenderTemplate(w, "search_user.html", data)
}
