package middleware

import "context"

type shutdownCtxKey struct{}

// WithShutdownContext attaches a server-shutdown context to ctx so long-lived
// streaming handlers (such as SSE) can terminate promptly when serving stops
// without cancelling ordinary in-flight request contexts.
func WithShutdownContext(ctx, shutdownCtx context.Context) context.Context {
	if ctx == nil || shutdownCtx == nil {
		return ctx
	}
	return context.WithValue(ctx, shutdownCtxKey{}, shutdownCtx)
}

// ShutdownContextFromContext extracts the server-shutdown context from ctx, if present.
func ShutdownContextFromContext(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	sc, _ := ctx.Value(shutdownCtxKey{}).(context.Context)
	return sc
}
