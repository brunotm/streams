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

func TestStreamBuild(t *testing.T) {
	builder, err := builder()
	assert.Nil(t, err)

	stream, err := builder.Build()
	assert.Nil(t, err)

	t.Log(stream.DotGraph())
}

func TestStreamStartStop(t *testing.T) {
	builder, err := builder()
	assert.Nil(t, err)

	builder.SetCloseTimeout(0)
	stream, err := builder.Build()
	assert.Nil(t, err)

	assert.Nil(t, stream.Start())
	assert.Nil(t, stream.Close())
}

func TestStreamContextScale(t *testing.T) {
	builder, err := builder()
	assert.Nil(t, err)

	builder.SetCloseTimeout(0)
	stream, err := builder.Build()
	assert.Nil(t, err)

	assert.Nil(t, stream.Start())
	assert.Nil(t, stream.Scale("source1", 4))
	assert.Nil(t, stream.Scale("source2", 10))
	assert.Nil(t, stream.Scale("source1", 1))
	assert.Nil(t, stream.Scale("source2", 2))
	assert.Nil(t, stream.Close())
}

func BenchmarkContextWalk(b *testing.B) {

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder, err := builder()
		if err != nil {
			b.Fatal("failed to build stream", err)
		}

		builder.SetCloseTimeout(0)
		stream, err := builder.Build()
		if err != nil {
			b.Fatal("failed to build stream", err)
		}

		stream.Start()
		time.Sleep(2 * time.Millisecond)
		stream.Close()
	}
}

func builder() (builder *Builder, err error) {

	builder = New("test-stream")
	err = builder.AddSource("source1", dummySource())
	if err != nil {
		return nil, err
	}
	err = builder.AddSource("source2", dummySource())
	if err != nil {
		return nil, err
	}
	err = builder.AddProcessor("processor1.1", dummyProcessor(), "source1")
	if err != nil {
		return nil, err
	}
	err = builder.AddProcessor("processor1.2", dummyProcessor(), "source1", "source2", "processor1.1")
	if err != nil {
		return nil, err
	}
	err = builder.AddProcessor("processor2.1", dummyProcessor(), "source2")
	if err != nil {
		return nil, err
	}
	err = builder.AddProcessor("processor2.2", dummyProcessor(), "processor2.1", "processor1.2")
	if err != nil {
		return nil, err
	}
	err = builder.AddSink("sink1", dummySink(), "source1", "source2", "processor2.2", "processor1.2")
	if err != nil {
		return nil, err
	}
	err = builder.AddSink("sink2", dummySink(), "processor2.1", "processor2.2", "processor1.2")
	if err != nil {
		return nil, err
	}

	return builder, nil

}

func dummySource() ProcessorSupplierFunc {
	pb := func() ProcessorFunc {
		pf := func(ctx *Context, rec Record) (err error) {
			for x := 0; x < 8192; x++ {
				select {
				case <-ctx.Done():
					return nil
				default:
					rec = Record{
						ID:    87783783798739,
						Topic: "awesome-topic",
						Key:   []byte(`awesome-key`),
						Value: []byte(`awesome-value`),
						Time:  time.Now(),
					}
					ctx.Forward(rec)
				}
			}
			return nil
		}
		return pf
	}
	return pb
}

func dummyProcessor() ProcessorSupplierFunc {
	pb := func() ProcessorFunc {
		pf := func(ctx *Context, rec Record) (err error) {
			ctx.Forward(rec)
			return nil
		}
		return pf
	}
	return pb
}

func dummySink() ProcessorSupplierFunc {
	pb := func() ProcessorFunc {
		pf := func(ctx *Context, rec Record) (err error) {
			return nil
		}
		return pf
	}
	return pb
}
