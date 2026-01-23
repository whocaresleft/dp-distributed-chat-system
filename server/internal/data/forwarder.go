package data

type Forwarder interface {
	ExecuteRemote(id string, action Action, epoch uint64, payload []byte) (*DataMessage, error)
}
