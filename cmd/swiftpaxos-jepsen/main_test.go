package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// fake answers each command it is sent after its own delay: first a
// reply to an earlier command, if stale is set, then the command's own.
// With first set, it answers before send returns the sequence number.
type fake struct {
	seq   int
	delay time.Duration
	stale bool
	first bool
	store map[int64][]byte
	rs    chan reply
}

func newFake() *fake {
	return &fake{store: map[int64][]byte{}, rs: make(chan reply, 16)}
}

func (f *fake) send(req request) (int, error) {
	seq := f.seq
	f.seq++
	var v []byte
	if req.F == "write" {
		f.store[req.Key] = encode(*req.Value)
	} else {
		v = f.store[req.Key]
	}
	answer := func() {
		if f.stale {
			f.rs <- reply{seq: seq - 1, value: encode(-1)}
		}
		f.rs <- reply{seq: seq, value: v}
	}
	if f.first {
		answer()
		return seq, nil
	}
	go func() {
		time.Sleep(f.delay)
		answer()
	}()
	return seq, nil
}

func (f *fake) replies() <-chan reply { return f.rs }

func run(t *testing.T, f *fake, lines ...string) []string {
	t.Helper()
	var out bytes.Buffer
	if err := serve(f, strings.NewReader(strings.Join(lines, "\n")+"\n"), &out, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(out.String()), "\n")
}

func TestWriteThenRead(t *testing.T) {
	got := run(t, newFake(),
		`{"f":"read","key":1}`,
		`{"f":"write","key":1,"value":7}`,
		`{"f":"read","key":1}`)
	want := []string{
		`{"type":"ok","value":null}`,
		`{"type":"ok","value":7}`,
		`{"type":"ok","value":7}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAReplyToAnEarlierCommandIsSkipped(t *testing.T) {
	f := newFake()
	f.stale = true
	got := run(t, f,
		`{"f":"write","key":1,"value":7}`,
		`{"f":"read","key":1}`)
	if got[1] != `{"type":"ok","value":7}` {
		t.Fatalf("read took another command's reply: %q", got)
	}
}

func TestAReplyBeforeItsSequenceNumberIsKept(t *testing.T) {
	f := newFake()
	f.first = true
	f.stale = true
	got := run(t, f,
		`{"f":"write","key":1,"value":7}`,
		`{"f":"read","key":1}`)
	want := []string{
		`{"type":"ok","value":7}`,
		`{"type":"ok","value":7}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A rejected line sends nothing, so the commands after it keep their
// replies.
func TestARejectedLineSendsNothing(t *testing.T) {
	for _, bad := range []struct{ line, want string }{
		{`{"f":"cas","key":1}`, `{"type":"fail","value":null,"error":"unknown-f"}`},
		{`{"f":"write","key":1}`, `{"type":"fail","value":null,"error":"write-without-value"}`},
	} {
		f := newFake()
		got := run(t, f,
			bad.line,
			`{"f":"write","key":1,"value":7}`,
			`{"f":"read","key":1}`)
		want := []string{bad.want, `{"type":"ok","value":7}`, `{"type":"ok","value":7}`}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("got %q, want %q", got, want)
		}
		if f.seq != 2 {
			t.Fatalf("%s sent a command", bad.line)
		}
	}
}

func TestATimeoutIsInfoForAWriteAndEndsTheSession(t *testing.T) {
	f := newFake()
	f.delay = time.Second
	got := run(t, f,
		`{"f":"write","key":1,"value":7}`,
		`{"f":"read","key":1}`)
	if len(got) != 1 || got[0] != `{"type":"info","value":null,"error":"timeout"}` {
		t.Fatalf("got %q", got)
	}
}

func TestATimeoutIsFailForARead(t *testing.T) {
	f := newFake()
	f.delay = time.Second
	got := run(t, f, `{"f":"read","key":1}`)
	if got[0] != `{"type":"fail","value":null,"error":"timeout"}` {
		t.Fatalf("got %q", got)
	}
}

func TestValuesRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -5, 1 << 40} {
		got, err := decode(encode(v))
		if err != nil || got == nil || *got != v {
			t.Fatalf("%d came back as %v, %v", v, got, err)
		}
	}
	if _, err := decode([]byte{1, 2}); err == nil {
		t.Fatal("a 2-byte value decoded")
	}
}
