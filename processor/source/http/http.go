package http

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
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/brunotm/streams"
	"github.com/brunotm/streams/internal/httpserver"
)

// Config for http Source
type Config struct {
	httpserver.Config
	User       string
	Password   string
	Ackwoledge bool
	Topics     []string
}

// Source processor for http
type Source struct {
	server     *httpserver.Server
	ackwoledge bool
	config     Config
	topics     map[string]struct{}
	donech     chan struct{}
}

// Supplier fot http source processor
type Supplier struct {
	config Config
}

// New creates a new http source processor instance
func (s Supplier) New() streams.Processor {
	sp := &Source{}
	sp.server = httpserver.New(s.config.Config)
	sp.ackwoledge = s.config.Ackwoledge
	sp.donech = make(chan struct{})

	sp.topics = make(map[string]struct{}, len(s.config.Topics))
	for _, topic := range s.config.Topics {
		sp.topics[topic] = struct{}{}
	}

	return sp
}

// New creates a processor supplier for this source
func New(config Config) (ps streams.ProcessorSupplier, err error) {

	if config.Addr == "" {
		return nil, errors.New("empty address")
	}

	if len(config.Topics) == 0 {
		return nil, errors.New("empty topics")
	}

	return Supplier{config}, nil

}

// Start source
func (sp *Source) Start() (err error) {
	go sp.server.Start()
	return nil
}

// Close this source
func (sp *Source) Close() (err error) {
	sp.server.Close(context.Background())
	close(sp.donech)
	return nil
}

// Process starts this source processing
func (sp *Source) Process(ctx *streams.Context, record streams.Record) (err error) {

	handler := func(w http.ResponseWriter, r *http.Request, ps httpserver.Params) {
		topic := ps.ByName("topic")
		key := ps.ByName("key")

		if sp.topics != nil {
			if _, ok := sp.topics[topic]; !ok {
				ctx.Logger().Debugw("received record on unregistered topic", "topic", topic, "key", key)
				http.Error(w, "topic not registerd", http.StatusNotFound)
				r.Body.Close()
				return
			}
		}

		ctx.Logger().Debugw("received record", "topic", topic, "key", key)

		var buf bytes.Buffer
		valueSize, err := buf.ReadFrom(r.Body)
		r.Body.Close()

		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadGateway)
			return
		}

		if valueSize == 0 && key == "" {
			http.Error(w, "null record", http.StatusBadRequest)
			return
		}

		record = streams.NewRecord(topic, []byte(key), buf.Bytes(), time.Now())

		var ackCh chan struct{}
		if sp.ackwoledge {
			ackCh = make(chan struct{})
			record.Ack = func() error {
				close(ackCh)
				return nil
			}
		}

		if err = ctx.Forward(record); err != nil {
			http.Error(w, "error processing record", http.StatusInternalServerError)
			return
		}

		ctx.Logger().Debugw("forwarded", "topic", topic, "record", record.ID)

		if sp.ackwoledge && ackCh != nil {
			<-ackCh
			ctx.Logger().Debugw("acknowledge received", "topic", topic, "key", key)
		}

		http.Error(w, "delivered", http.StatusOK)
		return
	}

	if sp.config.User != "" && sp.config.Password != "" {

		sp.server.AddHandler(
			"POST", "/:topic",
			httpserver.BasicAuth(handler, sp.config.User, sp.config.Password))

		sp.server.AddHandler(
			"POST", "/:topic/:key",
			httpserver.BasicAuth(handler, sp.config.User, sp.config.Password))

	} else {

		sp.server.AddHandler("POST", "/:topic", handler)
		sp.server.AddHandler("POST", "/:topic/:key", handler)

	}

	<-sp.donech

	return nil
}
