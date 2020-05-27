package models

// A description of an error state
type ModelError struct {

	// A human readable description of the error state
	Description string `json:"description,omitempty"`

	// The internal number referencing the error state
	InternalCode string `json:"internalCode,omitempty"`
}
