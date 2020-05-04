package contextutils

import (
	"context"
)

// contextKey is the wrapper we use for the names of the keys we store in Contexts
type contextKey struct {
	name string
}

var contextKeyUser = &contextKey{"user"}

// WithContextUser adds the user to the context
func WithContextUser(ctx context.Context, user string) context.Context {
	return withContextKeyVal(ctx, contextKeyUser, user)
}

func withContextKeyVal(ctx context.Context, key *contextKey, val string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, key, val)
	return ctx
}

// GetContextUser returns the user id stored in the context. If there is no value stored, we log a Fatal error because
// there is a potential security problem.
func GetContextUser(ctx context.Context) string {
	return getContextKey(ctx, contextKeyUser)
}

func getContextKey(ctx context.Context, key *contextKey) string {
	val, ok := ctx.Value(key).(string)
	if ok && val != "" {
		return val
	}
	// TODO: Log not found
	//log.Fatalf(
	//	"Somehow we got context without a %q value. This means that the authHandler didn't work correctly and there may be a security problem.",
	//	key.name)
	return ""
}
