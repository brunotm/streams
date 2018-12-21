package streams

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
	"github.com/brunotm/streams/internal/httpserver"
)

// Streams provides management and a API for running streams
type Streams struct {
	streams map[string]*Streams
	server  *httpserver.Server
}

// func NewServer() (streams *Streams) {
// 	streams = &Streams{}
// 	streams.server = server.New("localhost:9090")
// 	streams.server.AddHandler("GET", "/metrics", promhttp.Handler())
// 	streams.server.Start()
// 	return streams
// }

// s.summary = prometheus.NewSummaryVec(
// 	prometheus.SummaryOpts{Name: "record_processing_time"},
// 	[]string{"context", "task", "processor"})

// prometheus.MustRegister(s.summary)

// start := time.Now()
// c.stream.summary.WithLabelValues(c.name, c.task, c.node.name).
// 			Observe(float64(time.Since(start).Seconds()))

// s.server.Close()
// 	prometheus.Unregister(s.summary)
