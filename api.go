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

// Initializer interface. Any Processor or Store that must be initialized before
// running tasks in the the stream must implement this interface.
type Initializer interface {
	Init(pc ProcessorContext) (err error)
}

// Closer interface. Any Processor or Store that must be closed on Stream
// termination must implement this interface.
type Closer interface {
	Close() (err error)
}

// ProcessorContext is a execution context within a stream. Provides stream,
// task and processor information, routing of records to children processors,
// access to configured stores and contextual logging.
type ProcessorContext interface {
	// NodeName returns the current node name.
	NodeName() (name string)
	// StreamName returns the stream name.
	StreamName() (name string)
	// Config returns the stream app configuration.
	Config() (config Config)
	// IsActive returns if this context is active and can forward records to the stream.
	IsActive() (active bool)
	// Store returns the store with the given name
	Store(name string) (store Store, err error)
	// Forward the record to the downstream processors. Can be called multiple times
	// within Processor.Process() in order to send correlated or windowed records.
	Forward(record Record) (err error)
	// ForwardTo is like forward, but it forwards the record only to the given node
	ForwardTo(to string, record Record) (err error)
	// Error emits a error event to be handled by the Stream.
	Error(err error, records ...Record)
}

// Processor of records in a Stream. Both processors and sinks must implement
// this interface.
type Processor interface {
	Process(pc ProcessorContext, record Record)
}

// Source is a source of records in a Stream.
type Source interface {
	Processor
	Consume(pc ProcessorContext)
}

// ProcessorSupplier instantiates Processors used to create a Stream topology,
// recreate them or clone a Stream.
// If further configuration is needed, the processor must implement the Initializer
// interface in order to initialize itself before the Stream start and
// access configuration parameters through the provided context.
type ProcessorSupplier func() Processor

// ProcessorFunc implements a Processor for a function type
type ProcessorFunc func(pc ProcessorContext, record Record)

// Process the given record
func (f ProcessorFunc) Process(pc ProcessorContext, record Record) {
	f(pc, record)
}

// SourceSupplier instantiates Sources used to create a Stream topology,
// recreate them or clone a Stream.
// If further configuration is needed, the source must implement the Initializer
// interface in order to initialize itself before the Stream start and
// access configuration parameters through the provided context.
type SourceSupplier func() Store
