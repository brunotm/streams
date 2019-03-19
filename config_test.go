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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigIsSet(t *testing.T) {
	c := NewConfig(nil)
	c.Set("a value", "a.nested.value.set.2")
	assert.True(t, c.IsSet("a.nested"), "a.nested")
	assert.True(t, c.IsSet("a.nested.value.set.2"), "a.nested.value.set.2")
	assert.False(t, c.IsSet("a.nested.value.set.8"), "a.nested.value.set.8")
}

func TestConfigSetGet(t *testing.T) {
	c := NewConfig(nil)

	c.Set("string", "a.nested.value")
	assert.Equal(
		t,
		"string",
		c.Get("a.nested.value").String("string"),
		"a.nested.value",
	)

	c.Set(1.5, "array.append.#")
	assert.Equal(
		t,
		1.5,
		c.Get("array.append.0").Float64(2.0),
		"array.append.0",
	)

	c.Set(1, "array.append.#.nested")
	assert.Equal(
		t,
		int64(1),
		c.Get("array.append.1.nested").Int64(2),
		"array.append.1.nested",
	)

	c.Set(true, "array.append.#.0")
	assert.Equal(
		t,
		true,
		c.Get("array.append.2.0").Bool(false),
		"array.append.2.0",
	)

	c.Set(5, "array.append.5.grow")
	assert.Equal(
		t,
		int64(5),
		c.Get("array.append.5.grow").Int64(2),
		"array.append.5.grow",
	)

	c.Set(true, "array.set.index.2")
	assert.Equal(
		t,
		true,
		c.Get("array.set.index.2").Bool(false),
		"array.set.index.2",
	)

	c.Set(20, "array.set.index.9")
	assert.Equal(
		t,
		uint64(20),
		c.Get("array.set.index.9").Uint64(5),
		"array.set.index.9",
	)

	assert.NotNil(
		t,
		c.Get("array.set.index").Array(),
		"array.set.index",
	)

	assert.NotNil(
		t,
		c.Get("array.set").Map(),
		"map",
	)

	c.Set("1ms", "array.set.index.0")
	assert.Equal(
		t,
		time.Millisecond,
		c.Get("array.set.index.0").Duration(time.Microsecond),
		"array.set.index.0",
	)

	dt, _ := time.Parse(time.RFC3339, "2019-02-24T15:04:05Z")
	c.Set("2019-02-24T15:04:05Z", "array.set.index.1")
	assert.Equal(
		t,
		dt,
		c.Get("array.set.index.1").Time(time.Now()),
		"array.set.index.1",
	)

}

func TestConfigGetDefaults(t *testing.T) {
	c := NewConfig(nil)

	assert.Equal(
		t,
		"default",
		c.Get("a.default.string.value").String("default"),
		"a.default.string.value",
	)

	assert.Equal(
		t,
		true,
		c.Get("a.default.bool.value").Bool(true),
		"a.default.bool.value",
	)

	assert.Equal(
		t,
		int64(10),
		c.Get("a.default.int.value").Int64(10),
		"a.default.int.value",
	)

	assert.Equal(
		t,
		float64(10),
		c.Get("a.default.float.value").Float64(10),
		"a.default.float.value",
	)

	assert.Equal(
		t,
		uint64(10),
		c.Get("a.default.uint.value").Uint64(10),
		"a.default.uint.value",
	)

	assert.Equal(
		t,
		time.Microsecond,
		c.Get("a.default.duration.value").Duration(time.Microsecond),
		"a.default.duration.value",
	)

	dt, _ := time.Parse(time.RFC3339Nano, time.RFC3339Nano)
	assert.Equal(
		t,
		dt,
		c.Get("a.default.time.value").Time(dt),
		"a.default.time.value",
	)

}

// func TestConfigGet(t *testing.T) {
// 	c := NewConfig(nil)
