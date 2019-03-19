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

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Record.
type Encoder interface {
	Encode() ([]byte, error)
}

// ByteEncoder implements the Encoder interface for byte slices
// so that they can be used as the Key or Value in a Record.
type ByteEncoder []byte

// Encode serializes the encoder data as []byte
func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}

// StringEncoder implements the Encoder interface for Go strings
// so that they can be used as the Key or Value in a Record.
type StringEncoder string

// Encode serializes the encoder data as []byte
func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}
