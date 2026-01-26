/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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

// Checks if the user u is inside group with uuid groupUUID, using gs as the service to retrieve the group's members
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

// Extracts, if possible, the uint64 from value v
// It tries to cast v to uint64 and int, returning the uint64 value
// Otherwise 0 is returned
func extractUint64(v any) uint64 {
	var x uint64
	if val, ok := v.(uint64); ok {
		x = val
	} else if val, ok := v.(int); ok {
		x = uint64(val)
	} else {
		x = 0
	}
	return x
}
