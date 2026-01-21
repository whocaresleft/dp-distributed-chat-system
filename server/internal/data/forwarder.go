package data

type Forwarder interface {
	ExecuteRemote(id string, payload []byte) (*DataMessage, error)
}
