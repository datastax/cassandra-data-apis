package errors

type InternalError struct {
	msg string
}

func (e *InternalError) Error() string {
	return e.msg
}

func NewInternalError(text string) error {
	return &InternalError{text}
}
