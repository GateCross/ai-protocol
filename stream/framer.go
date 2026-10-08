package stream

import (
	"bytes"
	"fmt"
	"strings"
)

// Framer incrementally decodes SSE events. Data received by Feed is not
// dispatched until a complete event delimiter is seen; Finish dispatches the
// last event at EOF, as SSE requires. Callers that also need exact wire bytes
// should retain them separately: Event intentionally contains only protocol
// fields, not id/retry or the original line endings.
type Framer struct {
	line  []byte
	event string
	data  []string
}

// Feed adds a chunk and calls fn for each complete event in it. The partial
// line and event are retained for the next call. The scanner's existing
// per-line limit is enforced before appending more bytes.
func (f *Framer) Feed(p []byte, fn func(Event) error) error {
	for len(p) > 0 {
		nl := bytes.IndexByte(p, '\n')
		if nl < 0 {
			if len(f.line)+len(p) > maxEventBytes {
				return fmt.Errorf("SSE line exceeds %d bytes", maxEventBytes)
			}
			f.line = append(f.line, p...)
			return nil
		}
		if len(f.line)+nl > maxEventBytes {
			return fmt.Errorf("SSE line exceeds %d bytes", maxEventBytes)
		}
		f.line = append(f.line, p[:nl]...)
		if err := f.processLine(f.line, fn); err != nil {
			f.line = f.line[:0]
			return err
		}
		f.line = f.line[:0]
		p = p[nl+1:]
	}
	return nil
}

// Finish processes an unterminated final line and dispatches any event still
// pending at EOF. It is safe to call on an empty or already-finished framer.
func (f *Framer) Finish(fn func(Event) error) error {
	if len(f.line) > 0 {
		if err := f.processLine(f.line, fn); err != nil {
			f.line = f.line[:0]
			return err
		}
		f.line = f.line[:0]
	}
	return f.flush(fn)
}

func (f *Framer) processLine(line []byte, fn func(Event) error) error {
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if len(line) == 0 {
		return f.flush(fn)
	}
	if line[0] == ':' {
		return nil
	}
	name, value, _ := bytes.Cut(line, []byte{':'})
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	switch string(name) {
	case "event":
		f.event = string(value)
	case "data":
		f.data = append(f.data, string(value))
	}
	return nil
}

func (f *Framer) flush(fn func(Event) error) error {
	if f.event == "" && len(f.data) == 0 {
		return nil
	}
	ev := Event{Event: f.event, Data: strings.Join(f.data, "\n")}
	f.event = ""
	f.data = f.data[:0]
	return fn(ev)
}
