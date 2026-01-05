package node

type NodeConfig struct {
	Id          uint   `json:"id"`
	Role        string `json:"role"`
	TrustFactor int    `json:"trust_factor"`
	Port        uint16 `json:"port"`
}
