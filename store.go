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
)

// ROStore is a read only key/value store
type ROStore interface {
	// Name returns this store name.
	Name() (name string)

	// Get value for the given key. The value must be copied if utilization outside
	// of callback is needed
	Get(key []byte, callback func(value []byte, err error) error) (err error)

	// Iter iterates the store within the given key range applying the callback
	// for the key value pairs. Returning a error causes the iteration to stop.
	// A nil from or to sets the iterator to the begining or end of Store.
	// Both from and to as nil iterates the whole store
	Iter(from, to []byte, callback func(key, value []byte) error) (err error)

	// Count returns the number if entries in the store.
	Count() (count int64, err error)
}

// Store is a read write key/value store.
type Store interface {
	ROStore

	// Set the value for the given key.
	// If TTL is greater than 0 it will set an expiry time for the key.
	Set(key, value []byte, ttl time.Duration) error

	// Set the given key/value sequences in a single batch.
	// If TTL is greater than 0 it will set an expiry time for the keys.
	SetAll(keys, values [][]byte, ttl time.Duration) error

	// Delete the given key and associated value
	Delete(key []byte) error

	// Delete the given keys in a single batch
	DeleteAll(keys [][]byte) error
}
