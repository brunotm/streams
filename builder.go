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

import "time"

const (
	// DefaultBufferSize for source contexts
	DefaultBufferSize = 1024
	// DefaultCloseTimeout how long we should wait for finishing the processing
	// of in-flight records in the stream
	DefaultCloseTimeout = 10 * time.Second
	// DefaultInitialScale for each source task context
	DefaultInitialScale = 1
)

// ProcessorConfig type
type ProcessorConfig struct {
	Name       string            // Processor name
	Type       ProcessorType     // Type of processor
	Scale      int               // Default scale of tasks of a Source processor context
	BufferSize int               // Default buffer size of a Source processor context
	Supplier   ProcessorSupplier // Supplier of this processor
	Parents    []string          // Topology parents of this processor
}

// Builder for Stream topologies
type Builder struct {
	name         string
	stores       map[string]Store
	topology     []ProcessorConfig
	closeTimeout time.Duration
}

// New creates a Stream Builder
func New(name string) (builder *Builder) {
	builder = &Builder{}
	builder.name = name
	builder.stores = make(map[string]Store)
	builder.closeTimeout = DefaultCloseTimeout
	return builder
}

// SetCloseTimeout specifies how long we should wait for finishing the processing
// of in-flight records in the stream
func (b *Builder) SetCloseTimeout(timeout time.Duration) {
	b.closeTimeout = timeout
}

// AddSource adds a Source Processor with the given name and ProcessorSupplier
// to the topology. Scale specifies the initial scale for its context.
func (b *Builder) AddSource(name string, pb ProcessorSupplier) (err error) {
	config := ProcessorConfig{
		Name:       name,
		Type:       TypeSource,
		Scale:      DefaultInitialScale,
		BufferSize: DefaultBufferSize,
		Supplier:   pb,
	}
	return b.AddNode(config)
}

// AddProcessor adds a Stream Processor with the given name, ProcessorSupplier
// and parents to the topology.
func (b *Builder) AddProcessor(name string, pb ProcessorSupplier, parents ...string) (err error) {
	config := ProcessorConfig{
		Name:     name,
		Type:     TypeProcessor,
		Supplier: pb,
		Parents:  parents,
	}
	return b.AddNode(config)
}

// AddSink adds a Sink Processor with the given name and ProcessorSupplier
// and parents to the topology.
func (b *Builder) AddSink(name string, pb ProcessorSupplier, parents ...string) (err error) {
	config := ProcessorConfig{
		Name:     name,
		Type:     TypeSink,
		Supplier: pb,
		Parents:  parents,
	}
	return b.AddNode(config)
}

// Build builds the Stream.
func (b *Builder) Build() (stream *Stream, err error) {
	if b.name == "" {
		return nil, errEmptyName
	}

	if len(b.topology) == 0 {
		return nil, errInvalidDag
	}

	stream = &Stream{name: b.name}
	stream.nodes = make(map[string]*node)
	stream.builder = b

	for i := range b.topology {
		n := &node{}
		n.name = b.topology[i].Name
		n.typE = b.topology[i].Type
		n.processor = b.topology[i].Supplier.New()

		stream.addNode(n, b.topology[i].Parents)
	}

	return stream, nil
}

// AddStore adds a RW Store to the Stream accessible to its Processors
func (b *Builder) AddStore(name string, store Store) (err error) {
	if _, ok := b.stores[store.Name()]; ok {
		return errInvalidDag
	}
	b.stores[store.Name()] = store
	return nil
}

// AddNode adds a processor to the topology
func (b *Builder) AddNode(config ProcessorConfig) (err error) {

	if config.Name == "" {
		return errEmptyName
	}

	switch config.Type {
	case TypeSource:
	case TypeProcessor:
	case TypeSink:
	default:
		return errInvalidProcessorType

	}

	if _, exists := b.getConfig(config.Name); exists {
		return errInvalidDag
	}

	if config.Type == TypeSource && len(config.Parents) > 0 {
		return errInvalidDag
	}

	if (config.Type == TypeProcessor || config.Type == TypeSink) && len(config.Parents) == 0 {
		return errInvalidDag
	}

	// Check parents topology
	for _, parent := range config.Parents {

		if config.Name == parent {
			return errInvalidDag
		}

		p, exists := b.getConfig(parent)
		if !exists {
			return errParentNotFound
		}

		if p.Type == TypeSink {
			return errInvalidDag
		}
	}

	b.topology = append(b.topology, config)
	return nil
}

func (b *Builder) getConfig(name string) (config ProcessorConfig, ok bool) {
	for i := range b.topology {
		if b.topology[i].Name == name {
			return b.topology[i], true
		}
	}
	return config, false
}
