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
	"strconv"
	"time"

	"github.com/brunotm/streams/log"
)

// Context is a execution context within a stream. Provides stream,
// task and processor information, routing of records to children processors,
// access to configured stores and contextual logging.
// A Context is not safe for concurrent use and must not be used
// outside a Processor.Process() call.
type Context struct {
	name      string
	task      string
	stream    *Stream
	node      *node
	childrens []*Context
	logger    log.Logger
	buffer    chan Record
	donech    chan struct{}
}

// Stream returns the stream name.
func (c *Context) Stream() (name string) {
	return c.stream.name
}

// Name of current execution context.
func (c *Context) Name() (name string) {
	return c.name
}

// Processor returns the current processor given name in the Stream.
func (c *Context) Processor() (name string) {
	return c.node.name
}

// Forward the record to the downstream processors. Can be called multiple times
// within Processor.Process() in order to send correlated or windowed records.
func (c *Context) Forward(record Record) (err error) {
	if len(c.node.downstream) == 0 || c.node.typE == TypeSink {
		// panic for now if invalid forward
		panic(errSinkForward)
	}

	// Add to buffer when forwarding from a source processor
	if c.node.typE == TypeSource && len(c.childrens) > 0 {
		c.buffer <- record
		return nil
	}

	// Descend from Processors
	// Keep current node before switching
	currNode := c.node

	for i := 0; i < len(currNode.downstream); i++ {
		c.node = currNode.downstream[i]

		if err = c.node.processor.Process(c, record); err != nil {
			c.logger.Errorw("processor error",
				"processor", c.node.name,
				"topic", record.Topic,
				"record_id", record.ID,
				"error", err)
			return err
		}

	}

	// Restore current node in context
	c.node = currNode

	return nil
}

// Store returns the store for the given name
func (c *Context) Store(name string) (store Store, err error) {
	store, exists := c.stream.stores[name]
	if !exists {
		return nil, errStoreNotFound
	}
	return store, nil
}

// Logger returns this context logger. This logger instance brings
// the context of the Stream, execution context and current task but not
// from the Processor itself, that must be added as a KV attribute.
func (c *Context) Logger() (logger log.Logger) {
	return c.logger
}

// Done returns a channel that will be closed on context termination.
func (c *Context) Done() (done <-chan struct{}) {
	return c.donech
}

func (c *Context) close() {
	close(c.donech)

	if len(c.buffer) > 0 {
		<-time.After(c.stream.builder.closeTimeout)
		if len(c.buffer) > 0 {
			c.logger.Debugw("closed with pending buffer records",
				"pending_records", len(c.buffer),
				"waited_seconds", 10)
		}
	}

	for i := 0; i < len(c.childrens); i++ {
		c.childrens[i].close()
	}
}

func (c *Context) scale(scale int) {
	currScale := len(c.childrens)

	if scale > currScale {
		for ; scale > currScale; currScale++ {
			c.addChild()
		}
	}

	if scale < currScale {
		for ; scale < currScale; currScale-- {
			c.childrens[currScale-1].close()
			<-c.childrens[currScale-1].Done()
			c.childrens = c.childrens[:currScale-1]
		}
	}
}

func (c *Context) addChild() {
	child := &Context{}
	child.name = c.name
	child.task = strconv.Itoa(len(c.childrens) + 1)
	child.stream = c.stream
	child.logger = log.New("stream", child.stream.name, "context_source", child.name, "task", child.task)
	child.donech = make(chan struct{})
	c.childrens = append(c.childrens, child)

	// This goroutine ties the child context
	// with the main context over the buffer channel.
	// The code below is left inlined explicitly.
	go func() {
		for {
			select {
			case <-child.donech:
				return
			case record := <-c.buffer:
				for i := 0; i < len(c.node.downstream); i++ {
					child.node = c.node.downstream[i]
					child.node.processor.Process(child, record)
				}
			}
		}
	}()
}

func (c *Context) start() {
	config, _ := c.stream.builder.getConfig(c.node.name)
	c.buffer = make(chan Record, config.BufferSize)
	c.scale(config.Scale)

	go func() {
		if err := c.node.processor.Process(c, Record{}); err != nil {
			c.logger.Errorw("source processor error", "name", c.name, "error", err)
			c.logger.Errorw("shutting down stream", "error", c.stream.Close())
			return

		}
	}()
}
