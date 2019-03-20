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

	"github.com/brunotm/streams/types"
)

var (
	// ErrInvalidForward is returned when forwarding from a non active context
	// i.e. outisde a processor.Process() call, forwarding from a processor with
	// no sucessors or from a sink processor. A source processor context is
	// always active.
	ErrInvalidForward = errors.New("invalid forward")

	// ErrStoreNotFound is returned when the Store requested store
	// doesn't exists in the Stream topology.
	ErrStoreNotFound = errors.New("store not found")

	errPredecessorNotFound = errors.New("predecessor not found")
	errInvalidTopology     = errors.New("invalid topology")
	errEmptyName           = errors.New("name cannot be empty")
	errNodeNotFound        = errors.New("node not found")
	errInvalidNodeType     = errors.New("invalid node type")
)

// Topology is an acyclic graph of sources, processors, and sinks.
// A source is a node in the graph that consumes data from one or more sources
// and forwards them to its successor nodes.
// A processor is a node in the graph that receives input records from upstream nodes,
// processes the records, and optionally forwarding new records to its downstream nodes.
// A sink is a node in the graph that receives records from upstream nodes and writes
// them to external stores, systems or adjacent streams.
// A Topology allows you to construct an acyclic graph of these nodes,
// and then passed into a new Streams instance that will then begin consuming,
// processing, and producing records.
type topology struct {
	roots  []*Node
	nodes  []*Node
	stores map[string]*Node
}

// AddSource adds a source processor to the topology
func (t *topology) addSource(name string, ps SourceSupplier) (err error) {
	return t.addNode(name, types.Source, ps)
}

// AddProcessor adds a stream processor to the topology
func (t *topology) addProcessor(name string, ps ProcessorSupplier, predecessors ...string) (err error) {
	return t.addNode(name, types.Processor, ps, predecessors...)
}

// AddProcessorFunc adds a stream processor function to the topology
func (t *topology) addProcessorFunc(name string, pf ProcessorFunc, predecessors ...string) (err error) {
	ps := func() Processor {
		return pf
	}

	return t.addNode(name, types.Processor, ps, predecessors...)
}

// AddSink adds a sink processor to the topology
func (t *topology) addSink(name string, ps ProcessorSupplier, predecessors ...string) (err error) {
	return t.addNode(name, types.Sink, ps, predecessors...)
}

// AddStore adds a state store to the topology
func (t *topology) addStore(name string, ps StoreSupplier) (err error) {
	return t.addNode(name, types.Store, ps)
}

// AddSinkFunc adds a sink processor function to the topology
func (t *topology) addSinkFunc(name string, pf ProcessorFunc, predecessors ...string) (err error) {
	ps := func() Processor {
		return pf
	}

	return t.addNode(name, types.Sink, ps, predecessors...)
}

// Clone this topology. Existing stores are shared with the clone, sources, processors and sinks
// will be instantiated with the respective suppliers.
func (t *topology) clone() (top *topology, err error) {
	top = &topology{}
	top.stores = t.stores

	for _, node := range t.nodes {

		var predecessors []string
		for _, predecessor := range node.predecessors {
			predecessors = append(predecessors, predecessor.name)
		}

		err = top.addNode(node.name, node.typ, node.supplier, predecessors...)
		if err != nil {
			return nil, err
		}
	}

	return top, nil
}

// DotGraph genereates a DOT graph representation of the topology
func (t *topology) dotGraph() (graph string) {

	sb := &strings.Builder{}
	sb.WriteString("digraph Topology {\nrankdir=LR;\n")

	for _, n := range t.nodes {
		for _, sucessor := range n.successors {
			sb.WriteString(fmt.Sprintf(`"%s" -> "%s"`, n.name, sucessor.name))
			sb.WriteString("\r\n")
		}
	}

	// TODO: represent stores in graph

	sb.WriteString("}\n")
	return sb.String()
}

// Walk the topology starting from the given node inclusive if inc == true,
// applying the provided callback.
func (t *topology) walk(from *Node, inc bool, cb func(*Node) bool) (ok bool) {
	if inc {
		if !cb(from) {
			return false
		}
	}

	for i := 0; i < len(from.successors); i++ {
		if !t.walk(from.successors[i], true, cb) {
			return false
		}
	}

	return true
}

func (t *topology) validate() (err error) {

	// Ensure all added sources have sucessors in the graph
	for x := range t.roots {
		if len(t.roots[x].successors) == 0 {
			return errInvalidTopology
		}
	}

	return nil
}

// AddNode to the topology.
func (t *topology) addNode(name string, typ types.Type, ps interface{}, predecessors ...string) (err error) {
	node := &Node{}
	node.name = name
	node.typ = typ
	node.supplier = ps

	// No empty node names allowed
	if node.name == "" {
		return errEmptyName
	}

	// Don't replace already added nodes with same name
	if t.getNode(node.name) != nil {
		return errInvalidTopology
	}

	// Ensure processors and sinks always have predecessors
	if (node.typ == types.Processor || node.typ == types.Sink) && len(predecessors) == 0 {
		return errInvalidTopology
	}

	for _, predecessorName := range predecessors {

		// Check if we break the dag with loops
		if node.name == predecessorName {
			return errInvalidTopology
		}

		// Predecessor inclusion must happens before
		predecessor := t.getNode(predecessorName)
		if predecessor == nil {
			return errPredecessorNotFound
		}

		// Ensure predecessors are Sources or Processors
		if predecessor.typ == types.Sink {
			return errInvalidTopology
		}

		predecessor.successors = append(predecessor.successors, node)
		node.predecessors = append(node.predecessors, predecessor)
	}

	t.nodes = append(t.nodes, node)

	if node.typ == types.Source {
		t.roots = append(t.roots, node)
	}

	return nil
}

func (t *topology) getNode(name string) (node *Node) {
	for i := 0; i < len(t.nodes); i++ {
		if t.nodes[i].name == name {
			return t.nodes[i]
		}
	}

	return nil
}
