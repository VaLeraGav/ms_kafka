package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextIdKey string

const reqIDKey contextIdKey = "request_id"

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.New().String()

		ctx := context.WithValue(r.Context(), reqIDKey, requestID)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}

func GetReqID(ctx context.Context) string {
	if reqID, ok := ctx.Value(reqIDKey).(string); ok {
		return reqID
	}
	return ""
}
