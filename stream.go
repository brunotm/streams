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
	"runtime"
	"sync"

	"github.com/brunotm/streams/types"
)

// Error generated by the stream components
type Error struct {
	Node   *Node
	Error  error
	Record []Record
}

// Stream represents an unbounded, continuously updating data set.
// It contains a topology defining the data processing to be done.
// A Stream can have multiple concurrent tasks over the same processor topology.
// Stream Sources, Processors and Sinks must be safe for concurrent use.
type Stream struct {
	mtx      sync.Mutex
	name     string
	config   Config
	tasks    nodeTasks
	topology topology
	handler  func(Error)
	donech   chan struct{}
}

// Start initializes the stores, sources, processors and sinks within the
// topology and starts the stream.
func (s *Stream) Start() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Initialize tasks initializing stream componentes
	s.initTasks()

	for _, node := range s.topology.stores {
		ctx := newContext(s)
		if err = node.init(ctx); err != nil {
			return err
		}
	}

	for _, node := range s.topology.nodes {
		if node.typ == types.Source {
			continue
		}

		ctx := newContext(s)
		if err = node.init(ctx); err != nil {
			return err
		}
	}

	for _, node := range s.topology.roots {
		ctx := newContext(s)
		if err = node.init(ctx); err != nil {
			return err
		}

		// start streaming
		node.context.activate()
		go node.processor.(Source).Consume(ctx)
	}

	return nil
}

// Close the stream.
// Closes all stream sources and its tasks in parallel, close all processors
// sequentially if their context is deactivated, close all sink processors and
// finally all stores.
func (s *Stream) Close() (err error) {
	// first close all sources
	for _, node := range s.topology.roots {
		if closer, ok := node.processor.(Closer); ok {
			if err = closer.Close(); err != nil {
				return err
			}
		}

		// close all source tasks
		s.tasks.setScale(s.config, node, 0)
	}

	// Close all processors
	for _, node := range s.topology.nodes {
		if node.typ != types.Processor {
			continue
		}

		if closer, ok := node.processor.(Closer); ok {
			for {
				if node.context.IsActive() {
					runtime.Gosched()
					continue
				}
				if err = closer.Close(); err != nil {
					return err
				}
			}
		}

		// close all processor tasks
		s.tasks.setScale(s.config, node, 0)
	}

	// Close all sinks
	for _, node := range s.topology.nodes {
		if node.typ != types.Sink {
			continue
		}

		if closer, ok := node.processor.(Closer); ok {
			for {
				if node.context.IsActive() {
					runtime.Gosched()
					continue
				}
				if err = closer.Close(); err != nil {
					return err
				}
			}
		}
	}

	// Close all stores
	for _, node := range s.topology.stores {
		if closer, ok := node.processor.(Closer); ok {
			if err = closer.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Store returns the store with the given name
func (s *Stream) Store(name string) (store ROStore, err error) {
	st, exists := s.topology.stores[name]
	if !exists {
		return nil, errStoreNotFound
	}

	return st.processor.(ROStore), nil
}

// initTasks for all source and processors that have successors.
// Sink nodes are ignored.
func (s *Stream) initTasks() {
	s.tasks = make(nodeTasks)

	for _, node := range s.topology.nodes {
		if len(node.successors) == 0 || node.typ == types.Sink {
			continue
		}
		t := &tasks{}
		s.tasks[node] = t
		scale := s.config.Get(node.name, "tasks", "count").Int(0)
		s.tasks.setScale(s.config, node, scale)
	}
}
