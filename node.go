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

import "github.com/brunotm/streams/types"

// Node of a topology. Can be a source, sink, or processor node.
type Node struct {
	name         string
	typ          types.Type
	context      *context
	processor    Processor
	supplier     interface{}
	successors   []*Node
	predecessors []*Node
}

// Name of node
func (n *Node) Name() (name string) {
	return n.name
}

// Predecessors of node
func (n *Node) Predecessors() (predecessors []*Node) {
	return n.predecessors
}

// Successors of node
func (n *Node) Successors() (sucessors []*Node) {
	return n.successors
}

// Type of node
func (n *Node) Type() (typ types.Type) {
	return n.typ
}

// forward the record to node successors
func (n *Node) forward(record Record) {

	// For each successor, increment its context activation
	// and process the current record decrementing its activation
	// afterwards.
	for i := 0; i < len(n.successors); i++ {
		n.successors[i].context.activate()
		n.successors[i].processor.Process(n.successors[i].context, record)
		n.successors[i].context.deactivate()
	}

	// n.context.stream.topology.walk(
	// 	n,
	// 	false,
	// 	func(sucessor *Node) (ok bool) {
	// 		sucessor.context.activate()
	// 		sucessor.processor.Process(sucessor.context, record)
	// 		sucessor.context.deactivate()
	// 		return true
	// 	})

}

// initialize the node and processor with the given context
func (n *Node) init(ctx *context) (err error) {
	n.context = ctx
	n.context.node = n

	// Instatiate the node processor
	switch n.typ {
	case types.Store:
		n.processor = n.supplier.(StoreSupplier)()

	case types.Source:
		n.processor = n.supplier.(SourceSupplier)()

	case types.Processor, types.Sink:
		n.processor = n.supplier.(ProcessorSupplier)()

	default:
		return errInvalidNodeType
	}

	// Initialize the processor with the node context
	if initializer, ok := n.processor.(Initializer); ok {
		if err = initializer.Init(ctx); err != nil {
			return err
		}
	}

	return nil
}
