package errors

type ConflictError struct {
	msg string
}

func (e *ConflictError) Error() string {
	return e.msg
}

func NewConflictError(text string) error {
	return &ConflictError{text}
}
