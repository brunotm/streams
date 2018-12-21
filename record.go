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

	"github.com/cespare/xxhash"
)

// Record represents a single record within a stream
type Record struct {
	ID    uint64       // ID is a internal ID calculated over the record Value
	Topic string       // Topic to wich this Record is associated
	Key   []byte       // Record Key
	Value []byte       // Record Value
	Time  time.Time    // Record time
	Data  interface{}  // Data is useful to forwarding non-serialized data between Processors
	Ack   func() error // Ack Record source of its processing. Initially no-op.
}

// NewRecord creates a new record with the given paramenters and creates
// a Record.ID if the record has nay content
func NewRecord(topic string, key, value []byte, ts time.Time) (record Record) {
	record.Topic = topic
	record.Key = key
	record.Value = value
	record.Time = ts
	record.Ack = defaultAck

	if len(record.Value) > 0 {
		record.ID = xxhash.Sum64(record.Value)
	}

	return record
}

// IsValid returns if this record contains any data
func (r Record) IsValid() (valid bool) {
	return (r.Key != nil || r.Value != nil) && r.Topic != ""
}

// Copy data into a new record preserving current acknowledgement
func (r Record) Copy() (record Record) {
	record.ID = r.ID
	record.Time = r.Time
	record.Ack = r.Ack

	record.Key = make([]byte, len(r.Key))
	copy(record.Key, r.Key)

	record.Value = make([]byte, len(r.Value))
	copy(record.Value, r.Value)

	return record

}

func defaultAck() (err error) {
	return err
}
