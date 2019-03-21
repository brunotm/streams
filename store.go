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
)

var (
	// ErrKeyNotFound is returned when a key is not found on a get from the store.
	ErrKeyNotFound = errors.New("key not found")
)

// Remover interface. Any Store that must clear its data
// or state must implement this interface.
// Remove must ensure releasing and closing of resources.
type Remover interface {
	Remove() (err error)
}

// StoreSupplier instantiates Stores used to create a Stream topology,
// recreate them or clone a Stream.
// If further configuration is needed, the store must implement the Initializer
// interface in order to initialize itself before the Stream start and
// access configuration parameters through the provided context.
type StoreSupplier func() Store

// ROStore is a read only key/value store
type ROStore interface {

	// Name returns this store name.
	Name() (name string)

	// Get value for the given key.
	Get(key []byte) (value []byte, err error)

	// Range iterates the store in byte-wise lexicographical sorting order
	// within the given key range applying the callback for the key value pairs.
	// Returning a error causes the iteration to stop.
	// A nil from or to sets the iterator to the begining or end of Store.
	// Setting both from and to as nil iterates the whole store.
	// The key and value must be treated as immutable and read-only.
	// Key and value bytes remain available only during the callback call and
	// must be copied if outside use is needed,
	Range(from, to []byte, callback func(key, value []byte) error) (err error)

	// RangePrefix iterates the store over a key prefix applying the callback
	// for the key value pairs. Returning a error causes the iteration to stop.
	// The key and value must be treated as immutable and read-only.
	// Key and value bytes remain available only during the callback call and
	// must be copied if outside use is needed,
	RangePrefix(prefix []byte, cb func(key, value []byte) error) (err error)
}

// Store is a read write key/value store.
type Store interface {
	Processor
	ROStore

	// Set the value for the given key.
	// If TTL is greater than 0 it will set an expiry time for the key.
	Set(key, value []byte) (err error)
	// Set(key, value []byte, ttl time.Duration) (err error)

	// Delete the given key and associated value
	Delete(key []byte) (err error)
}
