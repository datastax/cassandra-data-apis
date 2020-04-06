package auth

import "context"

type contextKey struct {
	name string
}

var authKey = &contextKey{"userOrRole"}

func WithContextUserOrRole(ctx context.Context, userOrRole string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authKey, userOrRole)
}

func ContextUserOrRole(ctx context.Context) string {
	if ctx != nil {
		if val, ok := ctx.Value(authKey).(string); ok {
			return val
		}

	}
	return ""
}

