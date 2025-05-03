package middleware

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
)

func NewLogger(log *zerolog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Info().
				Str("method", r.Method).
				Str("url", r.URL.RequestURI()).
				Str("user_agent", r.UserAgent()).
				Str("request_id", GetReqID(r.Context())).
				Msg("Incoming request")

			ww := NewWrapResponseWriter(w, r.ProtoMajor)

			t1 := time.Now()
			defer func() {
				if rec := recover(); rec != nil {
					log.Error().
						Int("status", ww.Status()).
						Interface("recover_info", rec).
						Bytes("debug_stack", debug.Stack()).
						Str("inf", ww.Body.String()).
						Str("request_id", GetReqID(r.Context())).
						Msg("log system error")

					http.Error(ww, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					return
				}

				var login *zerolog.Event
				status := ww.Status()
				if status >= 200 && status < 300 {
					login = log.Info()
				} else {
					login = log.Warn()
				}
				login.
					Int("status", ww.Status()).
					Str("inf", ww.Body.String()).
					Str("elapsed_ms", time.Since(t1).String()).
					Str("request_id", GetReqID(r.Context())).
					Msg("Request processed")
			}()

			next.ServeHTTP(ww, r)
		})

		return http.HandlerFunc(fn)
	}
}
