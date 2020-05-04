package models

// Credentials contains the username and password to use for authenticating against DSE
type Credentials struct {
	Username string `validate:"required"`
	Password string `validate:"required"`
}

// AuthTokenResponse contains the authToken to be used for all future requests
type AuthTokenResponse struct {
	AuthToken string `json:"authToken,omitempty"`
}
