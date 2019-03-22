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
	"time"

	"github.com/dgryski/go-wyhash"
)

// Record represents a single record within a stream
type Record struct {
	id    uint64       // ID is a internal ID calculated over the record Value
	Topic string       // Topic to wich this Record is associated
	Key   Encoder      // Record Key
	Value Encoder      // Record Value
	Time  time.Time    // Record time
	ack   func() error // Ack Record source of its processing. Initially no-op.
}

// NewRecord creates a new record. Key and ack are optional and can be set to nil.
func NewRecord(topic string, key, value Encoder, ts time.Time, ack func() error) (record Record) {
	record.Topic = topic
	record.Key = key
	record.Value = value
	record.Time = ts
	record.ack = ack

	switch {
	case key != nil:
		b, _ := record.Key.Encode()
		record.id = wyhash.Hash(b, 0)
	case value != nil:
		b, _ := record.Value.Encode()
		record.id = wyhash.Hash(b, 0)
	}

	return record
}

// Ack acknowledge the record source of its processing
func (r Record) Ack() (err error) {
	if r.ack != nil {
		return r.ack()
	}
	return nil
}

// IsValid returns if this record contains any data
func (r Record) IsValid() (valid bool) {
	return (r.Key != nil || r.Value != nil) && r.Topic != ""
}
