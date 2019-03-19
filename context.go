package streams

import (
	"sync/atomic"

	"github.com/brunotm/streams/types"
)

// Context is a execution context within a stream. Provides stream,
// task and processor information, routing of records to children processors,
// access to configured stores and contextual logging.
type context struct {
	active int32
	stream *Stream
	node   *Node
}

func newContext(s *Stream) (ctx *context) {
	ctx = &context{}
	ctx.stream = s
	return ctx
}

// NodeName returns the current node name.
func (c *context) NodeName() (name string) {
	return c.node.name
}

// StreamName returns the stream name.
func (c *context) StreamName() (name string) {
	return c.stream.name
}

// Config returns the stream app configuration.
func (c *context) Config() (config Config) {
	return c.stream.config
}

// IsActive returns if this context is active and can forward records to the stream.
func (c *context) IsActive() (active bool) {
	return atomic.LoadInt32(&c.active) > 0
}

// Store returns the store for the given name
func (c *context) Store(name string) (store Store, err error) {
	node, exists := c.stream.topology.stores[name]
	if !exists {
		return nil, errStoreNotFound
	}

	return node.processor.(Store), nil
}

// Error emits a error event to be handled by the Stream.
func (c *context) Error(err error, records ...Record) {
	if c.stream.handler != nil {
		c.stream.handler(Error{c.node, err, records})
	}
}

// Forward the record to the downstream processors. Can be called multiple times
// within Processor.Process() in order to send correlated or windowed records.
func (c *context) Forward(record Record) (err error) {

	if !c.IsActive() || (len(c.node.successors) == 0 || c.node.typ == types.Sink) {
		return errInvalidForward
	}

	c.stream.tasks.forwardFrom(c.node, record)
	return nil
}

// ForwardTo is like forward, but it forwards the record only to the given node
func (c *context) ForwardTo(to string, record Record) (err error) {

	if !c.IsActive() || (len(c.node.successors) == 0 || c.node.typ == types.Sink) {
		return errInvalidForward
	}

	return c.stream.tasks.forwardTo(to, record)
}

// activate increments this context activation count
// allowing the processor to forward records in the stream
func (c *context) activate() {
	atomic.AddInt32(&c.active, 1)
}

// deactivate decrements this context activation count
func (c *context) deactivate() {
	atomic.AddInt32(&c.active, -1)
}
