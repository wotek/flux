package client

// EventNotification describes a real-time domain event notification received via SSE.
type EventNotification struct {
	Type           string `json:"type"`
	ListIdentifier string `json:"list_identifier"`
	Description    string `json:"description"`
	Position       uint64 `json:"position"`
}
