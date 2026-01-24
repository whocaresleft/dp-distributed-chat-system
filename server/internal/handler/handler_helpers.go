package handler

import (
	"regexp"
	"server/internal/entity"
	"server/internal/service"
)

func validateTag(tag string) bool {
	re := regexp.MustCompile(`^[0-9]{3,6}`)
	return re.MatchString(tag)
}

func isUserInGroup(u entity.User, groupUUID string, lastEpoch uint64, gs service.GroupService) bool {
	members, _, err := gs.GetGroupMembers(groupUUID, lastEpoch)
	if err != nil {
		return false
	}
	isIn := false
	for _, member := range members {
		if member.UUID == u.UUID {
			isIn = true
			break
		}
	}
	return isIn
}
