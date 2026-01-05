package handler

import (
	"server/internal/rendering"
	"server/internal/service"
)

type UserHandler struct {
	userService service.UserService
	renderer    *rendering.PageRenderer
}

func NewUserHandler(userService service.UserService, renderer *rendering.PageRenderer) *UserHandler {
	return &UserHandler{userService, renderer}
}

/**
 * / Main page - login/register page
 *
 * POST /users                  CreateUser
 * GET  /users/{uuid}           GetUserByUUID, redirects to username, tag
 * GET  /users/{username}       ListUsersByName
 * GET  /users/{username}/{tag} GetUserByNameTag
 * DELETE /users/{uuid} oppure /users/{uuid}/{tag} SoftDeleteUser, if authorized
 *
 */
