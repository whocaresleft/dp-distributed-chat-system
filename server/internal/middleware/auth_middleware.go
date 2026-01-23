package middleware

import (
	"context"
	"net/http"
	"server/internal/entity"

	"github.com/gorilla/sessions"
)

func AuthMiddleware(store *sessions.CookieStore, next func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		session, err := store.Get(r, "auth-session")

		if err != nil { // Log the error
			http.Error(w, "Internal Server Error", http.StatusInternalServerError) // 500
			return                                                                 // Important: Return after error
		}
		user_uuid, ok1 := session.Values["user_uuid"].(string)
		username, ok2 := session.Values["username"].(string)
		tag, ok3 := session.Values["tag"].(string)

		if !(ok1 && ok2 && ok3) {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}

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
		ctx = context.WithValue(ctx, "epoch", epoch)
		r = r.WithContext(ctx)

		next(w, r)
	}
}
