package server

// sseEventPayload represents the structured JSON payload sent via Server-Sent Events.
type sseEventPayload struct {
	Type           string `json:"type"`
	ListIdentifier string `json:"list_identifier"`
	Description    string `json:"description"`
	Position       uint64 `json:"position"`
}
