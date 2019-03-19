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
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
)

// Config is a configuration object safe for concurrent gets but not for sets.
// Configuration items are specified by a path using a dot separated names
// for both setting and getting configuration values.
// Valid paths:
// a
// a.nest.key
// a.nest.key.array.# for set to append to an array
// a.nest.key.array.#.key for set to append to an array an nested element
// a.nest.key.array.2 for set or get the 3rd element from an array
// a.nest.key.array.2.key for set or get the 3rd element from an array an nested element
type Config struct {
	data interface{}
}

// NewConfig creates a config from a exiting map[string]interface{}
// or an empty Config if nil is provided
func NewConfig(data map[string]interface{}) (c Config) {
	if data == nil {
		data = make(map[string]interface{})
	}
	c.data = data
	return c
}

// IsSet returns true if path is set. Path can be a dot separated keys or
// a varidic list of keys representing the path within config.
func (c Config) IsSet(path ...string) (ok bool) {
	if len(path) == 1 {
		path = strings.Split(path[0], ".")
	}
	return search(c.data, path) != nil
}

// Get retrieves the config item for the given path. Path can be specified as dot
// separated key structure or a varidic list of keys representing the path within config.
func (c Config) Get(path ...string) (config Config) {
	if len(path) == 1 {
		path = strings.Split(path[0], ".")
	}
	return Config{search(c.data, path)}
}

// String returns the string value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a string value.
func (c Config) String(def string) (value string) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToStringE(c.data); err != nil {
		return def
	}
	return value
}

// Bool returns the bool value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a bool value.
func (c Config) Bool(def bool) (value bool) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToBoolE(c.data); err != nil {
		return def
	}
	return value
}

// Duration returns the time.Duration value for the current Config item
// or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a time.Duration value.
func (c Config) Duration(def time.Duration) (value time.Duration) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToDurationE(c.data); err != nil {
		return def
	}
	return value
}

// Time returns the time.Time value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a time.Time value.
func (c Config) Time(def time.Time) (value time.Time) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToTimeE(c.data); err != nil {
		return def
	}
	return value
}

// Float64 returns the float64 value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a float64 value.
func (c Config) Float64(def float64) (value float64) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToFloat64E(c.data); err != nil {
		return def
	}
	return value
}

// Int returns the int64 value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a int64 value.
func (c Config) Int(def int) (value int) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToIntE(c.data); err != nil {
		return def
	}
	return value
}

// Int64 returns the int64 value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a int64 value.
func (c Config) Int64(def int64) (value int64) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToInt64E(c.data); err != nil {
		return def
	}
	return value
}

// Uint returns the uint64 value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a uint64 value.
func (c Config) Uint(def uint) (value uint) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToUintE(c.data); err != nil {
		return def
	}
	return value
}

// Uint64 returns the uint64 value for the current Config item or a provided default.
// The default value is only returned if the current Config item is nil or
// if it fails to parse the current item to a uint64 value.
func (c Config) Uint64(def uint64) (value uint64) {
	if c.data == nil {
		return def
	}

	var err error
	if value, err = cast.ToUint64E(c.data); err != nil {
		return def
	}
	return value
}

// Array returns the config array for the given path, returns nil if path
// is not found or value is not an array.
func (c Config) Array() (value []Config) {
	if arr, ok := c.data.([]interface{}); ok {
		for x := 0; x < len(arr); x++ {
			value = append(value, Config{arr[x]})
		}
	}
	return value
}

// Map returns the config map for the given path, returns nil if path
// is not found or value is not an object.
func (c Config) Map() (value map[string]Config) {
	if m, ok := c.data.(map[string]interface{}); ok {
		value = make(map[string]Config)
		for k, v := range m {
			value[k] = Config{v}
		}
	}
	return value
}

// Set the value for the given path
func (c Config) Set(value interface{}, path ...string) {
	if len(path) == 1 {
		path = strings.Split(path[0], ".")
	}
	set(c.data, value, path)
}

// search and fetch the value for the given path, returns nil if not found
func search(source interface{}, path []string) (data interface{}) {
	data = source
	var ok bool

	for _, key := range path {

		switch tmp := data.(type) {

		case map[string]interface{}:
			if data, ok = tmp[key]; !ok {
				return nil
			}

		case []interface{}:
			idx, err := strconv.ParseInt(key, 10, 64)
			if err != nil || int(idx) > len(tmp) {
				return nil
			}
			data = tmp[idx]

		}
	}

	return data
}

// set the value for the given path creating any needed map or slice
func set(source, value interface{}, path []string) {
	m, ok := source.(map[string]interface{})
	if !ok || m == nil {
		return
	}

	for i := 0; i < len(path); i++ {
		currentKey := path[i]
		nextKey := ""
		if i < len(path)-1 {
			nextKey = path[i+1]
		}

		if idx, err := strconv.ParseInt(nextKey, 10, 64); err == nil || nextKey == "#" {
			i++ // advance path index as this is a slice hashtag or slice index

			tmp, _ := m[currentKey].([]interface{})

			// # appends to slice
			if nextKey == "#" {
				// create a nested map if path has more depth
				if i < len(path)-1 {
					next := make(map[string]interface{})
					tmp = append(tmp, next)
					m[currentKey] = tmp
					m = next
					continue
				}

				// append a flat value to a specific index
				tmp = append(tmp, value)
				m[currentKey] = tmp
				return
			}

			// Grow slice as needed
			if len(tmp)-1 < int(idx) {
				tmp = append(tmp, make([]interface{}, int(idx+1)-len(tmp))...)
			}

			// get or create a nested map if path has more depth
			if i < len(path)-1 {
				next, ok := tmp[idx].(map[string]interface{})
				if !ok {
					next = make(map[string]interface{})
					tmp[idx] = next
				}

				m[currentKey] = tmp
				m = next
				continue
			}

			// set a flat value to a specific index
			tmp[idx] = value
			m[currentKey] = tmp
			return
		}

		// get or create a nested map if path has more depth
		if i < len(path)-1 {
			next, ok := m[currentKey].(map[string]interface{})
			if !ok {
				next = make(map[string]interface{})
				m[currentKey] = next
			}

			m = next
			continue
		}

		// set a flat value to a specific key
		m[currentKey] = value
	}

}
