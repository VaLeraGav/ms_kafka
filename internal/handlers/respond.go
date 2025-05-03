package handlers

import (
	"encoding/json"
	"net/http"
)

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func ErrorHandle(w http.ResponseWriter, r *http.Request, code int, err error) {
	Respond(w, r, code, Response{Status: "error", Message: err.Error()})
}

func SuccessStrHandle(w http.ResponseWriter, r *http.Request, code int, message string) {
	Respond(w, r, code, Response{Status: "success", Message: message})
}

func Respond(w http.ResponseWriter, _ *http.Request, code int, data interface{}) {
	w.WriteHeader(code)
	if data != nil {
		err := json.NewEncoder(w).Encode(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
