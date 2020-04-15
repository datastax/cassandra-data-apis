package auth

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContextUserOrRole(t *testing.T) {
	assert.Equal(t, "", ContextUserOrRole(context.Background()))
	assert.Equal(t, "user1",
		ContextUserOrRole(WithContextUserOrRole(context.Background(), "user1")))
}
