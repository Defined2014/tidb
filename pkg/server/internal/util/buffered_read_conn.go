// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bufio"
	"io"
	"net"
	"time"
)

// DefaultReaderSize is the default size of bufio.Reader.
const DefaultReaderSize = 16 * 1024

// BufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type BufferedReadConn struct {
	net.Conn
	io.Reader
	finishHandshake bool
	rb              *bufio.Reader
}

// NewBufferedReadConn creates a BufferedReadConn.
func NewBufferedReadConn(conn net.Conn, r io.Reader) *BufferedReadConn {
	return &BufferedReadConn{
		Conn:            conn,
		Reader:          r,
		finishHandshake: false,
		rb:              bufio.NewReaderSize(r, DefaultReaderSize),
	}
}

// Read reads data from the connection.
func (conn BufferedReadConn) Read(b []byte) (n int, err error) {
	if conn.finishHandshake {
		return conn.rb.Read(b)
	}
	return conn.Conn.Read(b)
}

// SetReadDeadline is useless for bufferedReadConn.
func (conn BufferedReadConn) SetReadDeadline(t time.Time) error {
	if conn.finishHandshake {
		return nil
	}
	return conn.Conn.SetReadDeadline(t)
}

func (conn *BufferedReadConn) SetFinishHandShake() {
	conn.finishHandshake = true
}
