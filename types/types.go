package types

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

// Type of the stream components
type Type uint8

func (t Type) String() (name string) {
	switch t {
	case Stream:
		return "stream"
	case Source:
		return "source"
	case Processor:
		return "processor"
	case Sink:
		return "sink"
	case Store:
		return "store"
	}
	return "unknown"
}

const (
	// Stream type
	Stream = Type(0)
	// Source processors type
	Source = Type(1)
	// Processor type
	Processor = Type(2)
	// Sink processor type
	Sink = Type(3)
	// Store type
	Store = Type(4)
)
