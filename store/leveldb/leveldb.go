package leveldb

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
	"os"
	"path/filepath"

	"github.com/brunotm/streams"
	ldb "github.com/syndtr/goleveldb/leveldb"
	ldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	ldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	dopt *ldbopt.Options
	wopt *ldbopt.WriteOptions
	ropt *ldbopt.ReadOptions
)

// make sure we implement the needed interfaces
var _ streams.Initializer = (*DB)(nil)
var _ streams.Closer = (*DB)(nil)
var _ streams.Remover = (*DB)(nil)
var _ streams.Store = (*DB)(nil)
var _ streams.StoreSupplier = Supplier

// DB is a durable leveldb key value state store
type DB struct {
	ctx  streams.Context
	db   *ldb.DB
	path string
}

// Supplier for leveldb store
func Supplier() (store streams.Store) {
	return &DB{}
}

// Init store
func (d *DB) Init(ctx streams.Context) (err error) {
	d.ctx = ctx

	statePath, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return err
	}

	statePath = statePath + "/state"

	d.path = ctx.Config().
		Get(ctx.StreamName(), "state", "path").
		String(statePath) + "/" + ctx.NodeName()

	d.db, err = ldb.OpenFile(d.path, dopt)
	if err != nil {
		return err
	}

	return err
}

// Remove closes the store and erases its contents
func (d *DB) Remove() (err error) {
	if err = d.Close(); err != nil {
		return nil
	}
	return os.RemoveAll(d.path)
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

// Process store any forwarded record to the store
func (d *DB) Process(ctx streams.Context, record streams.Record) {

	if !record.IsValid() || record.Key == nil {
		ctx.Error(errors.New("invalid record to store"), record)
	}

	key, err := record.Key.Encode()
	if err != nil {
		ctx.Error(errors.New("error serializing record key"), record)
	}

	value, err := record.Value.Encode()
	if err != nil {
		ctx.Error(errors.New("error serializing record value"), record)
	}

	if err = d.Set(key, value); err != nil {
		ctx.Error(err, record)
	}
}

// Get value for the given key.
func (d *DB) Get(key []byte) (value []byte, err error) {
	value, err = d.db.Get(key, ropt)

	if err == ldb.ErrNotFound {
		return nil, streams.ErrKeyNotFound
	}

	return value, err
}

// Set value for the given key.
func (d *DB) Set(key, value []byte) (err error) {
	return d.db.Put(key, value, wopt)
}

// Delete value for the given key.
func (d *DB) Delete(key []byte) (err error) {
	return d.db.Delete(key, wopt)
}

// Range iterates the store within the given key range applying the callback
// for the key value pairs. Returning a error causes the iteration to stop.
// A nil from or to sets the iterator to the begining or end of Store.
// Setting both from and to as nil iterates the whole store
func (d *DB) Range(from, to []byte, cb func(key, value []byte) error) (err error) {
	rng := &ldbutil.Range{Start: from, Limit: to}
	iter := d.db.NewIterator(rng, ropt)
	defer iter.Release()

	for iter.Next() {
		if err = cb(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}

	return iter.Error()
}

// RangePrefix iterates the store over a key prefix applying the callback
// for the key value pairs. Returning a error causes the iteration to stop.
func (d *DB) RangePrefix(prefix []byte, cb func(key, value []byte) error) (err error) {
	iter := d.db.NewIterator(ldbutil.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		if err = cb(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}

	return iter.Error()
}
