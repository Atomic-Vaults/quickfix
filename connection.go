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
	"io"
	"time"
)

type writeDeadlineSetter interface {
	SetWriteDeadline(t time.Time) error
}

type readDeadlineSetter interface {
	SetReadDeadline(t time.Time) error
}

func writeLoop(connection io.Writer, messageOut chan []byte, log Log, writeTimeout time.Duration) {
	deadlineSetter, canSetWriteDeadline := connection.(writeDeadlineSetter)

	for {
		msg, ok := <-messageOut
		if !ok {
			return
		}

		if writeTimeout > 0 && canSetWriteDeadline {
			if err := deadlineSetter.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				log.OnEvent(err.Error())
				return
			}
		}

		if _, err := connection.Write(msg); err != nil {
			log.OnEvent(err.Error())
			return
		}
	}
}

func readLoop(parser *parser, msgIn chan fixIn, log Log, readTimeout time.Duration, deadlineSetter readDeadlineSetter) {
	defer close(msgIn)

	for {
		if readTimeout > 0 && deadlineSetter != nil {
			if err := deadlineSetter.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
				log.OnEvent(err.Error())
				return
			}
		}

		msg, err := parser.ReadMessage()
		if err != nil {
			log.OnEvent(err.Error())
			return
		}
		msgIn <- fixIn{msg, parser.lastRead}
	}
}
