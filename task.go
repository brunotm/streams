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
	"sync"

	"github.com/dgryski/go-jump"
)

// tasks are dedicated concurrent tasks and buffers for each source or processor
// node that has successors nodes which are the ones allowed to forward in the topology.
// Each task consists of a goroutine and buffer pair to which the forward requests
// from that node are routed to. Ordered processing of records per multiple goroutines
// are guaranteed by using a consistent hash with the the record.id and number of tasks, for
// assigning records with same id to the same task. record ids are generated by hashing the
// encoded key or lately value of a record.
type nodeTasks map[*Node]*tasks

type tasks struct {
	sync.RWMutex
	buffers []chan Record
}

// forwardFrom forwards the given record to the given node successors.
// If the node has associated tasks, forward the record to the appropriate one.
func (nt nodeTasks) forwardFrom(from *Node, record Record) {
	st := nt[from]

	// TODO: rework locking strategy for subtasks
	st.RLock()
	if buckets := len(st.buffers); buckets > 0 {
		// Ensure we always process records with same keys within the same task
		st.buffers[jump.Hash(record.id, buckets)] <- record
		st.RUnlock()
		return
	}
	st.RUnlock()

	// if node has no tasks
	from.forward(record)
}

func (nt nodeTasks) forwardTo(to string, record Record) (err error) {
	// TODO: need a map[node.name]node
	for node := range nt {
		if node.name == to {
			node.context.activate()
			node.processor.Process(node.context, record)
			node.context.deactivate()
			return nil
		}
	}

	return errNodeNotFound
}

// setScale scales the number of tasks to the given scale
func (nt nodeTasks) setScale(config Config, node *Node, scale int) {
	st := nt[node]
	st.Lock()
	defer st.RUnlock()

	currScale := len(st.buffers)

	// Increase the number of tasks for the given node.
	// a scale of 1 only adds a buffer with no scale.
	if scale > currScale {
		bufferSize := config.Get(node.name, "tasks", "buffer_size").Int(0)

		for ; scale > currScale; currScale++ {
			task := make(chan Record, bufferSize)
			st.buffers = append(st.buffers, task)
			go func() {
				for record := range task {
					node.forward(record)
				}
			}()
		}
	}

	if scale < currScale {
		for ; scale < currScale; currScale-- {
			close(st.buffers[currScale-1])
			st.buffers = st.buffers[:currScale-1]
		}
	}
}
