// Package binfmt converts binary message formats to JSON and back.
//
// The extension does all of its work - jsonPath, field extraction, matchers,
// tracking - on JSON bytes. A Format is the innermost codec layer that lets a
// binary protocol take part in that: inbound records are turned into JSON
// once, in the read loop, and outbound JSON payloads are turned into the wire
// format before framing.
//
// The package has no k6 dependency, so test servers can share it.
package binfmt

// Format is one binary message format.
type Format interface {
	// Name is the format's config name ("msgpack", "protobuf").
	Name() string
	// ToJSON decodes one record and appends the resulting JSON message(s) to
	// dst. Self-delimiting formats may yield several messages per record. On
	// error dst is returned unchanged.
	ToJSON(dst [][]byte, rec []byte) ([][]byte, error)
	// FromJSON encodes one JSON payload.
	FromJSON(js []byte) ([]byte, error)
}

// maxDepth bounds nesting on both directions: hostile input must not be able
// to exhaust the goroutine stack.
const maxDepth = 100
