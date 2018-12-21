package log

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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	config zap.Config
	root   *zap.Logger
	logger *zap.SugaredLogger
)

func init() {
	var err error
	config = zap.NewProductionConfig()
	config.EncoderConfig = zap.NewProductionEncoderConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.Sampling = nil
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // "2006-01-02T15:04:05.000Z0700"
	// root, err = config.Build(zap.AddCallerSkip(1))
	root, err = config.Build()
	if err != nil {
		panic(err)
	}
	logger = root.Sugar()
}

// rfc3339TimeEncoder
func rfc3339TimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(time.RFC3339Nano))
}

// Logger interface
type Logger interface {
	Infow(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Debugw(msg string, keysAndValues ...interface{})
}

// New returns a logger with the given structured context
func New(keysAndValues ...interface{}) Logger {
	return logger.With(keysAndValues...)
}

// SetDebug log level
func SetDebug() {
	config.Level.SetLevel(zap.DebugLevel)
}

// SetInfo log level
func SetInfo() {
	config.Level.SetLevel(zap.InfoLevel)
}

// SetWarn log level
func SetWarn() {
	config.Level.SetLevel(zap.WarnLevel)
}

// SetError log level
func SetError() {
	config.Level.SetLevel(zap.ErrorLevel)
}
