// Copyright (c) quickfixengine.org  All rights reserved.
//
// This file may be distributed under the terms of the quickfixengine.org
// license as defined by quickfixengine.org and appearing in the file
// LICENSE included in the packaging of this file.
//
// This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
// THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE.
//
// See http://www.quickfixengine.org/LICENSE for licensing information.
//
// Contact ask@quickfixengine.org if any conditions of this licensing
// are not clear to you.

package quickfix

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
)

func TestWriteLoop(t *testing.T) {
	writer := bytes.NewBufferString("")
	msgOut := make(chan []byte)

	go func() {
		msgOut <- []byte("test msg 1 ")
		msgOut <- []byte("test msg 2 ")
		msgOut <- []byte("test msg 3")
		close(msgOut)
	}()
	writeLoop(writer, msgOut, nullLog{}, 0)

	expected := "test msg 1 test msg 2 test msg 3"

	if writer.String() != expected {
		t.Errorf("expected %v got %v", expected, writer.String())
	}
}

func TestWriteLoopTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	msgOut := make(chan []byte)
	done := make(chan struct{})

	go func() {
		writeLoop(client, msgOut, nullLog{}, 20*time.Millisecond)
		close(done)
	}()

	// Large payload ensures write blocks with net.Pipe when peer does not read.
	msgOut <- bytes.Repeat([]byte("x"), 1<<20)

	select {
	case <-done:
		return
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected writeLoop to return after write deadline timeout")
	}
}

func TestReadLoop(t *testing.T) {
	msgIn := make(chan fixIn)
	stream := "hello8=FIX.4.0\x019=5\x01blah\x0110=103\x01garbage8=FIX.4.0\x019=4\x01foo\x0110=103\x01"

	parser := newParser(strings.NewReader(stream))
	go readLoop(parser, msgIn, nullLog{}, 0, nil)

	var tests = []struct {
		expectedMsg   string
		channelClosed bool
	}{
		{expectedMsg: "8=FIX.4.0\x019=5\x01blah\x0110=103\x01"},
		{expectedMsg: "8=FIX.4.0\x019=4\x01foo\x0110=103\x01"},
		{channelClosed: true},
	}

	for _, test := range tests {
		msg, ok := <-msgIn
		switch {
		case !ok && !test.channelClosed:
			t.Error("Channel unexpectedly closed")
			fallthrough
		case !ok && test.channelClosed:
			continue
		}

		if msg.bytes.String() != test.expectedMsg {
			t.Errorf("Expected %v got %v", test.expectedMsg, msg.bytes.String())
		}
	}
}

func TestReadLoopTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	msgIn := make(chan fixIn)
	parser := newParser(client)

	done := make(chan struct{})
	go func() {
		readLoop(parser, msgIn, nullLog{}, 20*time.Millisecond, client)
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected readLoop to return after read deadline timeout")
	}
}
