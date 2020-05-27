package endpoint

import (
	"net/http"
)

type InternalError struct {
	msg string
}

func (e *InternalError) Error() string {
	return e.msg
}

func (e *InternalError) StatusCode() int {
	return http.StatusNotFound
}
