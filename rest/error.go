package rest

import (
	wallet "github.com/marselester/distributed-payment"
)

// General API errors.
const (
	errInvalidJSON = wallet.Error("problems parsing JSON")
	errInternal    = wallet.Error("internal error")
)

// apiError defines wallet API errors. For example, amount validation error could have
// "amount_lt_min" code and "Ensure this value is greater than 0." message.
// For generic errors code is optional.
type apiError struct {
	Message string `json:"message"`
	// Code helps a client to show improved error message in UI, e.g., i18n and better formatting.
	Code string `json:"code,omitempty"`
}

func (e apiError) Error() string {
	return e.Message
}
