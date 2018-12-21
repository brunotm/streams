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

// ProcessorType specifies the type of the stream processor
type ProcessorType string

const (
	// TypeSink processor type
	TypeSink ProcessorType = "sink"
	// TypeSource processor type
	TypeSource ProcessorType = "source"
	// TypeProcessor processor type
	TypeProcessor ProcessorType = "processor"
)

// Starter interface. Any Processor or Store that must be initialized before
// running tasks in the the stream must implement this interface.
type Starter interface {
	Start() error
}

// Closer interface. Any Processor or Store that must be closed on Stream
// termination must implement this interface.
type Closer interface {
	Close() error
}

// Processor of records in a Stream. Must be safe to concurrent use.
// Source, Stream and Sink Processors must implement this interface.
// For Sources the Process() method will be called once when the stream starts
// and must only return when the source is closed or on error.
// A Processor implementation must have a clear distinction of what consists
// an error and should be reflected in the Stream, from a record processing failure,
// i.e something that should be logged but don't has a significant impact in the
// Stream availability and correctness.
type Processor interface {
	Process(ctx *Context, record Record) (err error)
}

// ProcessorFunc provides a function type to create a Processor.
type ProcessorFunc func(ctx *Context, record Record) (err error)

// Process implements Processor for ProcessorFunc.
func (p ProcessorFunc) Process(ctx *Context, record Record) (err error) {
	return p(ctx, record)
}

// ProcessorSupplier interface. Instantiates configured Processors used to create
// for a Stream topology, recreate of individual processors on error or clone a Stream.
type ProcessorSupplier interface {
	New() Processor
}

// ProcessorSupplierFunc provides a func type for ProcessorSupplier
type ProcessorSupplierFunc func() ProcessorFunc

// New implements ProcessorSupplier for ProcessorSupplierFunc
func (fn ProcessorSupplierFunc) New() Processor {
	return fn()
}
