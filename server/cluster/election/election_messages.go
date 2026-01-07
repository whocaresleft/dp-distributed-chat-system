package election

/*
type YoPhase bool

const (
	Down YoPhase = false
	Up   YoPhase = true
)

func (y *YoPhase) String() string {
	if *y == Down {
		return "Down"
	}
	return "Up"
}

func (y *YoPhase) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", (*y).String())), nil
}

func (y *YoPhase) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	switch s {
	case "Up":
		*y = Up
	case "Down":
		*y = Down
	default:
		return fmt.Errorf("Wrongly formatted value")
	}
	return nil
}

type YoMessage struct {
	Phase YoPhase     `json:"phase"`
	Value node.NodeId `json:"value"`
	Round uint        `json:"round"`
}
*/
