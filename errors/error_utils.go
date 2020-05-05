package errors

import (
	"errors"
	"strings"

	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
)

// TranslateValidatorError takes an error from the go-playground validator (internally just a map of errors) and converts it into a string
// which can then be used to create a new error. The purpose of this function is to get around the fact that go-playground
// validator creates errors that are not in a user friendly format.
func TranslateValidatorError(err error, trans ut.Translator) error {
	switch err.(type) {
	case validator.ValidationErrors:
		errs := (err.(validator.ValidationErrors)).Translate(trans)

		vals := make([]string, 0, len(errs))

		for _, value := range errs {
			vals = append(vals, value)
		}

		return errors.New(strings.Join(vals, " "))
	default:
		return err
	}
}
