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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/brunotm/streams/log"
)

var (
	errStreamClosed         = errors.New("stream already closed")
	errParentNotFound       = errors.New("parent not found")
	errStoreNotFound        = errors.New("store not found")
	errInvalidDag           = errors.New("invalid dag")
	errSinkForward          = errors.New("cannot forward from a sink processor")
	errInvalidScale         = errors.New("invalid scale")
	errTaskNotFound         = errors.New("task not found")
	errEmptyName            = errors.New("name cannot be empty")
	errInvalidProcessorType = errors.New("invalid processor type")
)

type node struct {
	name       string
	typE       ProcessorType
	processor  Processor
	downstream []*node
}

// ContextInfo represents the context current info
type ContextInfo struct {
	Name       string
	Scale      int
	BufferSize int
}

// Stream represents an unbounded, continuously updating data set.
// It contains a topology defining the data processing to be done.
// A Stream can have multiple concurrent tasks over the same processor topology.
// Stream Sources, Processors and Sinks must be safe for concurrent use.
type Stream struct {
	mtx      sync.Mutex
	name     string
	closed   bool
	donech   chan struct{}
	contexts []*Context
	nodes    map[string]*node
	stores   map[string]Store
	// summary  *prometheus.SummaryVec
	builder *Builder
}

// Start the Stream with given task concurrency scale.
func (s *Stream) Start() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.closed {
		return errStreamClosed
	}

	for _, s := range s.stores {
		if s, ok := s.(Starter); ok {
			if err = s.Start(); err != nil {
				return err
			}
		}
	}

	for _, n := range s.nodes {
		if n.typE == TypeSource {
			continue
		}

		if s, ok := n.processor.(Starter); ok {
			if err = s.Start(); err != nil {
				return err
			}
		}
	}

	for _, n := range s.nodes {
		if n.typE != TypeSource {
			continue
		}

		if s, ok := n.processor.(Starter); ok {
			if err = s.Start(); err != nil {
				return err
			}
		}

		context := &Context{}
		context.name = n.name
		context.stream = s
		context.task = "0"
		context.logger = log.New("stream", s.name,
			"context_source", context.name, "task", context.task)

		context.node = n
		context.donech = make(chan struct{})
		s.contexts = append(s.contexts, context)
		context.start()
	}

	return nil
}

// Close the Stream and all running tasks releasing all resources
func (s *Stream) Close() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.closed {
		return errStreamClosed
	}

	s.closed = true

	for _, c := range s.contexts {
		if s, ok := c.node.processor.(Closer); ok {
			if err = s.Close(); err != nil {
				return err
			}
		}

		c.close()
		<-c.Done()
	}

	for _, n := range s.nodes {
		if n.typE == TypeSource {
			continue
		}

		if s, ok := n.processor.(Closer); ok {
			if err = s.Close(); err != nil {
				return err
			}
		}
	}

	for _, n := range s.nodes {
		if s, ok := n.processor.(Closer); ok {
			if err = s.Close(); err != nil {
				return err
			}
		}
	}

	for _, s := range s.stores {
		if s, ok := s.(Closer); ok {
			if err = s.Close(); err != nil {
				return err
			}
		}
	}

	s.contexts = nil
	s.nodes = nil
	s.stores = nil
	s.builder = nil

	return nil
}

// Contexts returns the contexts information
func (s *Stream) Contexts() (contexts []ContextInfo, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.closed {
		return nil, errStreamClosed
	}

	for i := range s.contexts {
		info := ContextInfo{}
		info.Name = s.contexts[i].name
		info.Scale = len(s.contexts[i].childrens) + 1
		info.BufferSize = cap(s.contexts[i].buffer)
		contexts = append(contexts, info)
	}

	return contexts, nil
}

// Scale the specified context in the Stream. A scale of 0 means that
// only the source task will traverse the processor topology unbuffered.
// A scales above 0 will use the Context buffer to distribute work between tasks.
func (s *Stream) Scale(name string, scale int) (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.closed {
		return errStreamClosed
	}

	if scale < 1 {
		return errInvalidScale
	}

	for i := range s.contexts {
		if s.contexts[i].name == name {
			s.contexts[i].scale(scale)
			return nil
		}
	}

	return errTaskNotFound
}

// DotGraph genereates a DOT graph representation of the Stream
func (s *Stream) DotGraph() (graph string) {

	sb := &strings.Builder{}
	sb.WriteString("digraph STREAM {\nrankdir=LR;\n")

	for _, n := range s.nodes {
		if len(n.downstream) == 0 {
			continue
		}

		for _, child := range n.downstream {
			sb.WriteString(fmt.Sprintf(`"%s" -> "%s"`, n.name, child.name))
			sb.WriteString("\r\n")
		}
	}

	sb.WriteString("}\n")
	return sb.String()
}

func (s *Stream) addNode(n *node, parents []string) {

	for _, parent := range parents {
		s.nodes[parent].downstream = append(s.nodes[parent].downstream, n)
	}

	s.nodes[n.name] = n
}

func (s *Stream) walk(from *node, fn func(*node) bool) (ok bool) {
	if !fn(from) {
		return false
	}

	for i := 0; i < len(from.downstream); i++ {
		if !s.walk(from.downstream[i], fn) {
			return false
		}
	}

	return true
}
