package streams

import (
	"sync/atomic"

	"github.com/brunotm/streams/types"
)

// ProcessorContext is a execution processorContext within a stream. Provides stream,
// task and processor information, routing of records to children processors,
// access to configured stores and contextual logging.
type processorContext struct {
	active int32
	stream *Stream
	node   *Node
}

func newContext(s *Stream) (pc *processorContext) {
	pc = &processorContext{}
	pc.stream = s
	return pc
}

// NodeName returns the current node name.
func (pc *processorContext) NodeName() (name string) {
	return pc.node.name
}

// StreamName returns the stream name.
func (pc *processorContext) StreamName() (name string) {
	return pc.stream.name
}

// Config returns the stream app configuration.
func (pc *processorContext) Config() (config Config) {
	return pc.stream.config
}

// IsActive returns if this context is active and can forward records to the stream.
func (pc *processorContext) IsActive() (active bool) {
	return atomic.LoadInt32(&pc.active) > 0
}

// Store returns the store for the given name
func (pc *processorContext) Store(name string) (store Store, err error) {
	node, exists := pc.stream.topology.stores[name]
	if !exists {
		return nil, ErrStoreNotFound
	}

	return node.processor.(Store), nil
}

// Error emits a error event to be handled by the Stream.
func (pc *processorContext) Error(err error, records ...Record) {
	if pc.stream.handler != nil {
		pc.stream.handler(Error{pc.node, err, records})
	}
}

// Forward the record to the downstream processors. Can be called multiple times
// within Processor.Process() in order to send correlated or windowed records.
func (pc *processorContext) Forward(record Record) (err error) {

	if !pc.IsActive() || (len(pc.node.successors) == 0 || pc.node.typ == types.Sink) {
		return ErrInvalidForward
	}

	pc.stream.tasks.forwardFrom(pc.node, record)
	return nil
}

// ForwardTo is like forward, but it forwards the record only to the given node
func (pc *processorContext) ForwardTo(to string, record Record) (err error) {

	if !pc.IsActive() {
		return ErrInvalidForward
	}

	return pc.stream.tasks.forwardTo(to, record)
}

// activate increments this context activation count
// allowing the processor to forward records in the stream
func (pc *processorContext) activate() {
	atomic.AddInt32(&pc.active, 1)
}

// deactivate decrements this context activation count
func (pc *processorContext) deactivate() {
	atomic.AddInt32(&pc.active, -1)
}
