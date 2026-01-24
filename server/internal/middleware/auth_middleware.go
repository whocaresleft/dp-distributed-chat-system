package middleware

import (
	"context"
	"net/http"
	"server/internal/entity"

	"github.com/gorilla/sessions"
)

// Middleware used to check if the user has an open session on the site, manually authenticating him if so.
func AuthMiddleware(store *sessions.CookieStore, next func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// Tries to retrieve an auth-session
		session, err := store.Get(r, "auth-session")
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		// Extracts the user's information from the cookie
		user_uuid, ok1 := session.Values["user_uuid"].(string)
		username, ok2 := session.Values["username"].(string)
		tag, ok3 := session.Values["tag"].(string)

		// If at least one is not set, the user has to log in, otherwise he is automatically authenticated (we reopen the active session).
		if !(ok1 && ok2 && ok3) {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}

		// Checks for the system epoch (trying to parse it as both int or uint64).
		// This makes sure the client-side has partial knowledge on the temporal state of the server (he knows that at least <epoch> operations)
		// changed the system up until now. It's mainly used to send with any request so that we can check if the request we received is valid (e.g. not in the future, with clientEpoch > serverEpoch)
		var epoch uint64
		if val, ok := session.Values["last-seen-epoch"].(uint64); ok {
			epoch = val
		} else if val, ok := session.Values["last-seen-epoch"].(int); ok {
			epoch = uint64(val)
		}

		user := entity.User{
			UUID:     user_uuid,
			Username: username,
			Tag:      tag,
		}

		ctx := context.WithValue(r.Context(), "user", user)
		ctx = context.WithValue(ctx, "epoch", epoch) // Keeping it separate for convenience
		r = r.WithContext(ctx)

		next(w, r)
	}
}
