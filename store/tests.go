package store

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
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/brunotm/streams"
	"github.com/stretchr/testify/assert"
)

// TestStore for streams.Store implementations
func TestStore(t *testing.T, supplier streams.StoreSupplier, ctx streams.Context) {
	var err error
	var store streams.Store

	key := randStringBytes(8)
	value := randStringBytes(32)

	t.Run("initialize", func(t *testing.T) {
		store = supplier()
		if initializer, ok := store.(streams.Initializer); ok {
			err = initializer.Init(ctx)
		}
		assert.NoError(t, err)
	})

	t.Run("get inexistent key", func(t *testing.T) {
		_, err = store.Get(key)
		assert.Equal(t, err, streams.ErrKeyNotFound)
	})

	t.Run("set", func(t *testing.T) {
		err = store.Set(key, value)
		assert.NoError(t, err)

		v, err := store.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, bytes.Compare(v, value), 0)
	})

	t.Run("get", func(t *testing.T) {
		v, err := store.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, bytes.Compare(v, value), 0)
	})

	t.Run("delete", func(t *testing.T) {
		err = store.Delete(key)
		assert.NoError(t, err)
	})

	t.Run("get deleted key", func(t *testing.T) {
		_, err = store.Get(key)
		assert.Equal(t, err, streams.ErrKeyNotFound)
	})

	// create a random set of keys
	keys := make([][]byte, 10)
	for x := 0; x < 10; x++ {
		keys[x] = randStringBytes(4)
	}

	// create a sorted copy of keys
	sorted := make([][]byte, 10)
	copy(sorted, keys)
	sort.Slice(sorted, func(i int, j int) bool {
		return string(sorted[i]) < string(sorted[j])
		// switch bytes.Compare(sorted[i], sorted[j]) {
		// case -1:
		// 	return true
		// case 0, 1:
		// 	return false
		// default:
		// 	log.Panic("not fail-able with `bytes.Comparable` bounded [-1, 1].")
		// 	return false
		// }
	})

	t.Run("range lexicographical", func(t *testing.T) {
		for x := len(keys) - 1; x >= 0; x-- {
			store.Set(keys[x], value)
		}

		idx := 1
		err = store.Range(sorted[1], sorted[3], func(key, value []byte) error {
			assert.Equal(t, bytes.Compare(key, sorted[idx]), 0)
			idx++
			return nil
		})

		assert.NoError(t, err)
		err = store.Delete(key)
		assert.NoError(t, err)
	})

	t.Run("range all lexicographical", func(t *testing.T) {
		for x := len(keys) - 1; x >= 0; x-- {
			store.Set(keys[x], value)
		}

		idx := 0
		err = store.Range(nil, nil, func(key, value []byte) error {
			assert.Equal(t, bytes.Compare(key, sorted[idx]), 0)
			idx++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, idx, len(sorted))

		err = store.Delete(key)
		assert.NoError(t, err)
	})

	t.Run("concurrent set and get", func(t *testing.T) {
		start := make(chan struct{})

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			<-start
			for x := 0; x < 100; x++ {
				for i := 0; i < len(sorted); i++ {
					_, err := store.Get(sorted[i])
					assert.NoError(t, err)
				}
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			close(start)
			for x := 0; x < 100; x++ {
				for i := 0; i < len(sorted); i++ {
					err := store.Set(keys[i], value)
					assert.NoError(t, err)
				}
			}
			wg.Done()
		}()

		wg.Wait()
	})

	// Range is expected to see all keys
	t.Run("concurrent delete and range", func(t *testing.T) {
		start := make(chan struct{})

		var wg sync.WaitGroup
		var count int

		wg.Add(1)
		go func() {
			<-start
			for i := 0; i < len(sorted); i++ {
				err := store.Delete(sorted[i])
				assert.NoError(t, err)
			}

			wg.Done()
		}()

		wg.Add(1)
		go func() {
			close(start)
			err := store.Range(nil, nil, func(key, value []byte) error {
				count++
				return nil
			})
			assert.NoError(t, err)
			wg.Done()
		}()
		wg.Wait()
		assert.Equal(t, len(sorted), count)
	})

}

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

func randString(n int) string {
	return string(randStringBytes(n))
}
