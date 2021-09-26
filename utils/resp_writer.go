package utils

import (
	"bytes"
	"net/http"
)

var _ http.ResponseWriter = &ResponseWriter{}

type ResponseWriter struct {
	Body    *bytes.Buffer
	Headers http.Header
	Code    int
}

func NewRespWriter(body *bytes.Buffer, header http.Header) ResponseWriter {
	return ResponseWriter{
		Body:    body,
		Headers: header,
		Code:    200,
	}
}

func (r *ResponseWriter) Header() http.Header {
	return r.Headers
}

func (r *ResponseWriter) Write(b []byte) (int, error) {
	return r.Body.Write(b)
}

func (r *ResponseWriter) WriteHeader(statusCode int) {
	r.Code = statusCode
}
