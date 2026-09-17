package flux

// Actor represents the entity that initiated a change in the system.
type Actor struct {
	// Identifier is the globally unique ID of the actor (e.g., urn:...:user/123 or urn:...:service/auth).
	Identifier Identifier
}
