package mock

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

	"github.com/brunotm/streams"
)

// make sure we implement the Context interfaces
var _ streams.Context = (*Context)(nil)

// ContextData for mocking
type ContextData struct {
	Active         bool
	NodeName       string
	StreamName     string
	Config         streams.Config
	Store          streams.Store
	ErrorCount     int
	ForwardCount   int
	ForwardToCount int
}

// Context mock
type Context struct {
	Data ContextData
}

// NodeName returns the current node name.
func (c *Context) NodeName() (name string) {
	return c.Data.NodeName
}

// StreamName returns the stream name.
func (c *Context) StreamName() (name string) {
	return c.Data.StreamName
}

// Config returns the stream app configuration.
func (c *Context) Config() (config streams.Config) {
	return c.Data.Config
}

// IsActive returns if this context is active and can forward records to the stream.
func (c *Context) IsActive() (active bool) {
	return c.Data.Active
}

// Store returns the store with the given name
func (c *Context) Store(name string) (store streams.Store, err error) {
	return c.Data.Store, nil
}

// Forward the record to the downstream processors. Can be called multiple times
// within Processor.Process() in order to send correlated or windowed records.
func (c *Context) Forward(record streams.Record) (err error) {
	if !c.Data.Active {
		return errors.New("invalid forward")
	}

	c.Data.ForwardCount++
	return nil
}

// ForwardTo is like forward, but it forwards the record only to the given node
func (c *Context) ForwardTo(to string, record streams.Record) (err error) {
	if !c.Data.Active {
		return errors.New("invalid forward")
	}

	c.Data.ForwardToCount++
	return nil
}

// Error emits a error event to be handled by the Stream.
func (c *Context) Error(err error, records ...streams.Record) {
	c.Data.ErrorCount++
}
