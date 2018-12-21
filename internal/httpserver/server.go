package httpserver

/*
   Copyright 2018 Bruno Moura <brunotm@gmail.com>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

import (
	"context"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
)

// Config for http Source
type Config struct {
	Addr              string
	WriteTimeout      time.Duration
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
}

// Server is a api server for a stream
type Server struct {
	config Config
	http   *http.Server
	router *httprouter.Router
}

// New Server
func New(config Config) (server *Server) {
	server = &Server{}
	server.config = config
	server.router = httprouter.New()
	server.http = &http.Server{}
	server.http.Addr = config.Addr

	if config.WriteTimeout != 0 {
		server.http.WriteTimeout = config.WriteTimeout
	}

	if config.ReadTimeout != 0 {
		server.http.ReadTimeout = config.ReadTimeout
	}

	if config.ReadHeaderTimeout != 0 {
		server.http.ReadHeaderTimeout = config.ReadHeaderTimeout
	}

	server.http.Handler = server.router
	return server
}

// Start serving
func (s *Server) Start() {
	go func() {
		s.http.ListenAndServe()
		err := s.http.ListenAndServe()
		if err != http.ErrServerClosed {
			panic(err)
		}
	}()
}

// Close serving
func (s *Server) Close(ctx context.Context) (err error) {
	return s.http.Shutdown(ctx)
}

// AddHandler adds a handler for the given method and path
func (s *Server) AddHandler(method, path string, handler Handle) {
	s.router.Handle(method, path, handler)
}

// BasicAuth middleware
func BasicAuth(h Handle, requiredUser, requiredPassword string) Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user, password, hasAuth := r.BasicAuth()
		if hasAuth && user == requiredUser && password == requiredPassword {
			h(w, r, ps)
		} else {
			w.Header().Set("WWW-Authenticate", "Basic realm=Restricted")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		}
	}
}

// Handle is a http handler
type Handle = httprouter.Handle

// Params from the URL
type Params = httprouter.Params
