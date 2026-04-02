package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	log.Printf("server listening on %s", listener.Addr())

	go runServer(listener)

	oldConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		log.Fatalf("dial old conn failed: %v", err)
	}
	defer oldConn.Close()

	log.Printf("client opened OLD connection")

	var writeCount uint64
	var lastProgress int64
	atomic.StoreInt64(&lastProgress, time.Now().UnixNano())

	oldWriterDone := make(chan error, 1)
	go func() {
		payload := bytes.Repeat([]byte("X"), 1<<20)
		for {
			if _, err := oldConn.Write(payload); err != nil {
				oldWriterDone <- err
				return
			}
			atomic.AddUint64(&writeCount, 1)
			atomic.StoreInt64(&lastProgress, time.Now().UnixNano())
		}
	}()

	if !waitUntilLikelyBlocked(&writeCount, &lastProgress, oldWriterDone, 15*time.Second) {
		log.Printf("could not reliably force a blocked write in time on this machine")
		return
	}

	log.Printf("OLD connection writer appears blocked")

	newConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		log.Fatalf("dial new conn failed: %v", err)
	}
	defer newConn.Close()

	log.Printf("client opened NEW connection")

	if _, err := io.WriteString(newConn, "PING\n"); err != nil {
		log.Fatalf("write on new conn failed: %v", err)
	}

	reply, err := bufio.NewReader(newConn).ReadString('\n')
	if err != nil {
		log.Fatalf("read on new conn failed: %v", err)
	}

	log.Printf("NEW connection round-trip succeeded: %q", reply)

	select {
	case err := <-oldWriterDone:
		log.Printf("OLD writer unexpectedly unblocked early: %v", err)
	case <-time.After(2 * time.Second):
		log.Printf("OLD writer is still blocked even though NEW connection works")
	}

	if err := oldConn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		log.Fatalf("set write deadline failed: %v", err)
	}

	select {
	case err := <-oldWriterDone:
		log.Printf("OLD writer unblocked after deadline with error: %v", err)
	case <-time.After(2 * time.Second):
		log.Printf("OLD writer still not finished; exiting demo")
	}
}

func waitUntilLikelyBlocked(writeCount *uint64, lastProgress *int64, done <-chan error, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return false
		case err := <-done:
			log.Printf("old writer ended before blocking: %v", err)
			return false
		case <-ticker.C:
			count := atomic.LoadUint64(writeCount)
			idleFor := time.Since(time.Unix(0, atomic.LoadInt64(lastProgress)))
			if count > 0 && idleFor > 2*time.Second {
				return true
			}
		}
	}
}

func runServer(listener net.Listener) {
	var connID int

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		connID++
		id := connID

		switch id {
		case 1:
			log.Printf("server accepted OLD connection; blackholing reads")
			go func(c net.Conn) {
				defer c.Close()
				select {}
			}(conn)
		default:
			log.Printf("server accepted NEW connection")
			go func(c net.Conn) {
				defer c.Close()
				reader := bufio.NewReader(c)
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				_, _ = io.WriteString(c, "ACK:"+line)
			}(conn)
		}
	}
}
