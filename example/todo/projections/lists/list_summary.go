package lists

// ListSummary represents the high-level summary of a todo list aggregate in the read model.
type ListSummary struct {
	Identifier string `json:"identifier"`
	Title      string `json:"title"`
	Active     int    `json:"active"`
	Archived   int    `json:"archived"`
}
