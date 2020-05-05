package errors

type NotFoundError struct {
	msg string
}

func (e *NotFoundError) Error() string {
	return e.msg
}

func NewNotFoundError(text string) error {
	return &NotFoundError{text}
}
