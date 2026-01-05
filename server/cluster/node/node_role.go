package node

import (
	"encoding/json"
	"strings"
)

type NodeRole uint

const (
	Leader NodeRole = iota
	Persistence
	Input
	Follower
)

var roleToName = map[NodeRole]string{
	Leader:      "leader",
	Persistence: "persistence",
	Input:       "input",
	Follower:    "follower",
}

var nameToRole = map[string]NodeRole{
	"leader":      Leader,
	"persistence": Persistence,
	"input":       Input,
	"follower":    Follower,
}

func (role *NodeRole) String() string {
	return roleToName[*role]
}

func (role *NodeRole) MarshalJSON() ([]byte, error) {
	return json.Marshal(role.String())
}

func (role *NodeRole) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	*role = nameToRole[strings.ToLower(s)]
	return nil
}
