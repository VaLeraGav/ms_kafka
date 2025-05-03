package middleware

import (
	"bytes"
	"net/http"
)

type WrapResponseWriter struct {
	http.ResponseWriter
	StatusCode int
	Body       *bytes.Buffer
}

func NewWrapResponseWriter(w http.ResponseWriter, protoMajor int) *WrapResponseWriter {
	return &WrapResponseWriter{
		ResponseWriter: w,
		StatusCode:     http.StatusOK,
		Body:           new(bytes.Buffer),
	}
}
func (ww *WrapResponseWriter) Write(b []byte) (int, error) {
	ww.Body.Write(b)
	return ww.ResponseWriter.Write(b)
}

func (ww *WrapResponseWriter) WriteHeader(statusCode int) {
	ww.StatusCode = statusCode
	ww.ResponseWriter.WriteHeader(statusCode)
}

func (ww *WrapResponseWriter) Status() int {
	return ww.StatusCode
}
