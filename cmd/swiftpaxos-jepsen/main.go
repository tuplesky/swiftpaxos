// swiftpaxos-jepsen: one SwiftPaxos client session for a Jepsen process,
// driven over JSON lines, as coord-jepsen drives TupleSky.
//
// It connects the upstream client (the one `swiftpaxos -run client`
// runs, with the SwiftPaxos protocol's reply rules) through the master,
// prints a ready line, then takes one request per line on stdin and
// answers each with one line on stdout:
//
//	{"f":"write","key":3,"value":7}  ->  {"type":"ok","value":7}
//	{"f":"read","key":3}             ->  {"type":"ok","value":7}  (null: never written)
//
// An operation not answered within -timeout-ms is "info" for a write (it
// may still take effect) and "fail" for a read (it has none). Either way
// the shim exits after answering: the upstream client keeps one reply
// value for the session, so a late reply to the timed-out command could be
// taken for the next command's, and a fresh session (a new client id)
// cannot mix them up.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/imdea-software/swiftpaxos/client"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/swift"
)

type request struct {
	F     string `json:"f"`
	Key   int64  `json:"key"`
	Value *int64 `json:"value"`
}

type answer struct {
	Type  string `json:"type"`
	Value *int64 `json:"value"`
	Error string `json:"error,omitempty"`
}

// session is what an operation needs from a connected client: send a
// command, and the replies as they are delivered, each tagged with the
// sequence number of its command.
type session interface {
	send(req request) error
	replies() <-chan reply
}

type reply struct {
	seq   int
	value []byte
}

// encode and decode a register value: 8 bytes, little-endian.
func encode(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decode(b []byte) (*int64, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) != 8 {
		return nil, fmt.Errorf("a value of %d bytes", len(b))
	}
	v := int64(binary.LittleEndian.Uint64(b))
	return &v, nil
}

// perform sends the seq-th command of the session and waits for its reply.
// Replies to earlier commands are skipped. It says whether the session is
// still usable.
func perform(s session, seq int, req request, timeout time.Duration) (answer, bool) {
	switch req.F {
	case "read":
	case "write":
		if req.Value == nil {
			return answer{Type: "fail", Error: "write-without-value"}, true
		}
	default:
		return answer{Type: "fail", Error: "unknown-f"}, true
	}
	unknown := "info"
	if req.F == "read" {
		unknown = "fail"
	}
	// A send can block on a replica that does not read (paused), so it
	// runs on its own and counts against the timeout.
	sent := make(chan error, 1)
	go func() { sent <- s.send(req) }()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case err := <-sent:
			if err != nil {
				return answer{Type: unknown, Error: "send: " + err.Error()}, false
			}
		case r := <-s.replies():
			if r.seq != seq {
				continue
			}
			if req.F == "write" {
				return answer{Type: "ok", Value: req.Value}, true
			}
			v, err := decode(r.value)
			if err != nil {
				return answer{Type: "fail", Error: err.Error()}, false
			}
			return answer{Type: "ok", Value: v}, true
		case <-deadline.C:
			return answer{Type: unknown, Error: "timeout"}, false
		}
	}
}

// serve answers requests from in until it ends or an answer leaves the
// session unusable.
func serve(s session, in io.Reader, out io.Writer, timeout time.Duration) error {
	scanner := bufio.NewScanner(in)
	enc := json.NewEncoder(out)
	for seq := 0; scanner.Scan(); seq++ {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return fmt.Errorf("bad request line: %w", err)
		}
		a, ok := perform(s, seq, req, timeout)
		if err := enc.Encode(a); err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return scanner.Err()
}

// upstream is a session over the upstream SwiftPaxos client.
type upstream struct {
	b  *client.BufferClient
	rs chan reply
}

func (u *upstream) send(req request) error {
	if req.F == "write" {
		u.b.SendWrite(req.Key, encode(*req.Value))
	} else {
		u.b.SendRead(req.Key)
	}
	return nil
}

func (u *upstream) replies() <-chan reply { return u.rs }

// fastPing puts a `ping` first on PATH that sends one probe instead of
// three. The upstream client pings every replica on connecting, to find
// the closest; three probes a replica cost two seconds each, on every
// reconnect after an :info.
func fastPing(dir string) error {
	real, err := findOnPath("ping")
	if err != nil {
		return err
	}
	script := fmt.Sprintf("#!/bin/sh\nexec %s \"$1\" -c 1 -W 2 -q\n", real)
	if err := os.WriteFile(filepath.Join(dir, "ping"), []byte(script), 0o755); err != nil {
		return err
	}
	return os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func findOnPath(name string) (string, error) {
	for _, d := range filepath.SplitList(os.Getenv("PATH")) {
		p := filepath.Join(d, name)
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			return p, nil
		}
	}
	return "", errors.New(name + " is not on PATH")
}

func connect(server, master string, port, replicas int, logPath string) (*upstream, error) {
	logger := dlog.New(logPath, true)
	c := client.NewClientLog(server, master, port, true, false, true, logger)
	// The reply buffer outlasts any session: one reply a command.
	b := client.NewBufferClient(c, 1<<16, 8, 0, 0, 0)
	if err := b.Connect(); err != nil {
		return nil, err
	}
	if swift.NewClient(b, replicas) == nil {
		return nil, errors.New("no SwiftPaxos client")
	}
	u := &upstream{b: b, rs: make(chan reply, 16)}
	go func() {
		for r := range b.Reply {
			u.rs <- reply{seq: r.Seqnum, value: r.Val}
		}
	}()
	return u, nil
}

func main() {
	server := flag.String("server", "", "The replica this session is closest to, HOST:PORT")
	master := flag.String("master", "", "The master's host")
	port := flag.Int("master-port", 7087, "The master's port")
	replicas := flag.Int("replicas", 0, "How many replicas there are")
	timeoutMs := flag.Int("timeout-ms", 5000, "How long one operation may take")
	connectMs := flag.Int("connect-ms", 30000, "How long connecting may take")
	logPath := flag.String("log", "", "The client's log (default: none)")
	flag.Parse()

	ready := json.NewEncoder(os.Stdout)
	fail := func(why string) {
		ready.Encode(map[string]any{"ready": false, "error": why})
		os.Exit(1)
	}
	if *server == "" || *master == "" || *replicas < 1 {
		fail("-server, -master and -replicas are required")
	}
	// The upstream client logs to stdout unless it has a file.
	if *logPath == "" {
		*logPath = os.DevNull
	}
	tmp, err := os.MkdirTemp("", "swiftpaxos-jepsen-")
	if err != nil {
		fail(err.Error())
	}
	defer os.RemoveAll(tmp)
	if err := fastPing(tmp); err != nil {
		fail(err.Error())
	}

	type result struct {
		u   *upstream
		err error
	}
	done := make(chan result, 1)
	go func() {
		u, err := connect(*server, *master, *port, *replicas, *logPath)
		done <- result{u, err}
	}()
	var u *upstream
	select {
	case r := <-done:
		if r.err != nil {
			fail("connect: " + r.err.Error())
		}
		u = r.u
	case <-time.After(time.Duration(*connectMs) * time.Millisecond):
		fail("connect: timeout")
	}
	ready.Encode(map[string]any{"ready": true})

	if err := serve(u, os.Stdin, os.Stdout, time.Duration(*timeoutMs)*time.Millisecond); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
