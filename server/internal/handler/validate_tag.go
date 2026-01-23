package handler

import "regexp"

func validateTag(tag string) bool {
	re := regexp.MustCompile(`^[0-9]{3,6}`)
	return re.MatchString(tag)
}
