package moss

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
	"bytes"
	"errors"

	"github.com/brunotm/streams"
	"github.com/couchbase/moss"
)

var (
	ropts    = moss.ReadOptions{}
	wopts    = moss.WriteOptions{}
	iteropts = moss.IteratorOptions{}
)

// make sure we implement the needed interfaces
var _ streams.Initializer = (*DB)(nil)
var _ streams.Closer = (*DB)(nil)
var _ streams.Remover = (*DB)(nil)
var _ streams.Store = (*DB)(nil)
var _ streams.StoreSupplier = Supplier

// DB is a in-memory key value MOSS state store
type DB struct {
	ctx streams.Context
	db  moss.Collection
}

// Supplier for moss store
func Supplier() (store streams.Store) {
	return &DB{}
}

// Init store
func (d *DB) Init(ctx streams.Context) (err error) {
	d.ctx = ctx
	d.db, err = moss.NewCollection(moss.DefaultCollectionOptions)
	if err != nil {
		return err
	}
	return d.db.Start()
}

// Remove closes the store and erases its contents
func (d *DB) Remove() (err error) {
	return d.Close()
}

// Close the store releasing its resources.
func (d *DB) Close() (err error) {
	err = d.db.Close()
	d.db = nil
	return err
}

// Name returns this store name.
func (d *DB) Name() (name string) {
	return d.ctx.NodeName()
}

// Process store or deletes any forwarded record to the store.
// Records with empty values deletes the given key from the store.
func (d *DB) Process(ctx streams.Context, record streams.Record) {

	if !record.IsValid() || record.Key == nil {
		ctx.Error(errors.New("invalid record to store"), record)
		return
	}

	key, err := record.Key.Encode()
	if err != nil {
		ctx.Error(errors.New("error serializing record key"), record)
		return
	}

	// Records with empty values deletes the given key from the store.
	if record.Value == nil {
		if err = d.Delete(key); err != nil {
			ctx.Error(err, record)
		}
		return
	}

	value, err := record.Value.Encode()
	if err != nil {
		ctx.Error(errors.New("error serializing record value"), record)
		return
	}

	if err = d.Set(key, value); err != nil {
		ctx.Error(err, record)
	}
}

// Get value for the given key.
func (d *DB) Get(key []byte) (value []byte, err error) {
	value, err = d.db.Get(key, ropts)

	if value == nil && err == nil {
		return nil, streams.ErrKeyNotFound
	}

	return value, err
}

// Set value for the given key.
func (d *DB) Set(key, value []byte) (err error) {

	batch, err := d.db.NewBatch(1, len(key)+len(value))
	if err != nil {
		return err
	}
	defer batch.Close()

	err = batch.Set(key, value)
	if err != nil {
		return err
	}

	return d.db.ExecuteBatch(batch, wopts)
}

// Delete value for the given key.
func (d *DB) Delete(key []byte) (err error) {

	batch, err := d.db.NewBatch(1, 0)
	if err != nil {
		return err
	}
	defer batch.Close()

	// Moss returns a nil error on a non-existent key
	err = batch.Del(key)
	if err != nil {
		return err
	}

	return d.db.ExecuteBatch(batch, wopts)
}

// Range iterates the store within the given key range applying the callback
// for the key value pairs. Returning a error causes the iteration to stop.
// A nil from or to sets the iterator to the begining or end of Store.
// Setting both from and to as nil iterates the whole store
func (d *DB) Range(from, to []byte, cb func(key, value []byte) error) (err error) {

	ss, err := d.db.Snapshot()
	if err != nil {
		return err
	}

	iter, err := ss.StartIterator(from, to, iteropts)
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		key, val, err := iter.Current()
		if err != nil {
			if err == moss.ErrIteratorDone {
				return nil
			}
			return err
		}

		if err = cb(key, val); err != nil {
			return err
		}

		iter.Next()
	}

}

// RangePrefix iterates the store over a key prefix applying the callback
// for the key value pairs. Returning a error causes the iteration to stop.
func (d *DB) RangePrefix(prefix []byte, cb func(key, value []byte) error) (err error) {
	err = d.Range(nil, nil, func(key, value []byte) error {
		if bytes.HasPrefix(key, prefix) {
			return cb(key, value)
		}
		return nil
	})

	return err
}
