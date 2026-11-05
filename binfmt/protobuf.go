package binfmt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ProtoConfig describes a protobuf schema. Give either Source (.proto text,
// compiled in-process, no protoc needed) or DescriptorSet (the output of
// `protoc --descriptor_set_out=x.pb --include_imports`).
type ProtoConfig struct {
	Source string
	// FileName is Source's name for imports and error messages. Default "main.proto".
	FileName string
	// Imports are extra .proto files by import path. The google/protobuf/*
	// well-known types are built in.
	Imports map[string]string

	DescriptorSet []byte

	// Message is the full name of the inbound message, e.g. "shop.Envelope".
	Message string
	// SendMessage is the outbound message; default Message.
	SendMessage string

	// JSONNames renders fields by their lowerCamelCase JSON name. Default:
	// the names as written in the .proto, so paths match the schema.
	JSONNames bool
	// EmitDefaults renders fields that hold their zero value. Off by default,
	// per proto3 JSON; fields declared `optional` are rendered whenever set.
	EmitDefaults bool
}

// Protobuf decodes and encodes one message type with a schema. JSON follows
// the protobuf JSON mapping: 64-bit integers are strings, enums are names,
// bytes are base64. Output is compact.
type Protobuf struct {
	in, out protoreflect.MessageDescriptor
	mo      protojson.MarshalOptions
	uo      protojson.UnmarshalOptions
}

// Name implements Format.
func (*Protobuf) Name() string { return "protobuf" }

// NewProtobuf compiles or loads the schema. Compiled schemas are cached by
// content, so connecting with the same config per iteration is cheap.
func NewProtobuf(cfg ProtoConfig) (*Protobuf, error) {
	if cfg.Message == "" {
		return nil, errors.New("protobuf: proto.message is required (full name, e.g. pkg.Message)")
	}
	find, err := schema(cfg)
	if err != nil {
		return nil, err
	}
	in, err := messageDesc(find, cfg.Message)
	if err != nil {
		return nil, err
	}
	out := in
	if cfg.SendMessage != "" && cfg.SendMessage != cfg.Message {
		if out, err = messageDesc(find, cfg.SendMessage); err != nil {
			return nil, err
		}
	}
	return &Protobuf{
		in: in, out: out,
		mo: protojson.MarshalOptions{UseProtoNames: !cfg.JSONNames, EmitUnpopulated: cfg.EmitDefaults},
		uo: protojson.UnmarshalOptions{},
	}, nil
}

// ToJSON implements Format.
func (p *Protobuf) ToJSON(dst [][]byte, rec []byte) ([][]byte, error) {
	m := dynamicpb.NewMessage(p.in)
	if err := proto.Unmarshal(rec, m); err != nil {
		return dst, fmt.Errorf("protobuf: %s: %w", p.in.FullName(), err)
	}
	js, err := p.mo.Marshal(m)
	if err != nil {
		return dst, fmt.Errorf("protobuf: %s to JSON: %w", p.in.FullName(), err)
	}
	// protojson deliberately varies its whitespace between builds; compact
	// output keeps `contains` matchers predictable.
	var buf bytes.Buffer
	buf.Grow(len(js))
	if err := json.Compact(&buf, js); err != nil {
		return dst, err
	}
	return append(dst, buf.Bytes()), nil
}

// FromJSON implements Format.
func (p *Protobuf) FromJSON(js []byte) ([]byte, error) {
	m := dynamicpb.NewMessage(p.out)
	if err := p.uo.Unmarshal(js, m); err != nil {
		return nil, fmt.Errorf("protobuf: payload does not fit %s: %w", p.out.FullName(), err)
	}
	return proto.Marshal(m)
}

type finder func(protoreflect.FullName) (protoreflect.Descriptor, error)

func messageDesc(find finder, name string) (protoreflect.MessageDescriptor, error) {
	d, err := find(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("protobuf: message %q not found in the schema", name)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("protobuf: %q is not a message", name)
	}
	return md, nil
}

// Compiling a .proto takes milliseconds; a VU that connects every iteration
// must not pay that each time. The cache is bounded: past the limit schemas
// are still compiled, just not kept.
const schemaCacheMax = 64

var (
	schemaMu    sync.Mutex
	schemaCache = map[[32]byte]finder{}
)

func schema(cfg ProtoConfig) (finder, error) {
	key := schemaKey(cfg)
	schemaMu.Lock()
	f, ok := schemaCache[key]
	schemaMu.Unlock()
	if ok {
		return f, nil
	}

	var err error
	switch {
	case len(cfg.DescriptorSet) > 0:
		f, err = fromDescriptorSet(cfg.DescriptorSet)
	case cfg.Source != "":
		f, err = compileSource(cfg)
	default:
		err = errors.New("protobuf: give proto.source (.proto text) or proto.descriptorSet")
	}
	if err != nil {
		return nil, err
	}

	schemaMu.Lock()
	if len(schemaCache) < schemaCacheMax {
		schemaCache[key] = f
	}
	schemaMu.Unlock()
	return f, nil
}

func schemaKey(cfg ProtoConfig) [32]byte {
	h := sha256.New()
	put := func(s string) {
		fmt.Fprintf(h, "%d:", len(s))
		h.Write([]byte(s))
	}
	put(cfg.FileName)
	put(cfg.Source)
	names := make([]string, 0, len(cfg.Imports))
	for k := range cfg.Imports {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		put(k)
		put(cfg.Imports[k])
	}
	put(string(cfg.DescriptorSet))
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

func compileSource(cfg ProtoConfig) (finder, error) {
	name := cfg.FileName
	if name == "" {
		name = "main.proto"
	}
	srcs := make(map[string]string, len(cfg.Imports)+1)
	for k, v := range cfg.Imports {
		srcs[k] = v
	}
	srcs[name] = cfg.Source
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(srcs),
		}),
	}
	files, err := c.Compile(context.Background(), name)
	if err != nil {
		return nil, fmt.Errorf("protobuf: compiling %s: %w", name, err)
	}
	res := files.AsResolver()
	return res.FindDescriptorByName, nil
}

func fromDescriptorSet(b []byte) (finder, error) {
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(b, &set); err != nil {
		return nil, fmt.Errorf("protobuf: descriptorSet is not a FileDescriptorSet: %w", err)
	}
	files, err := protodesc.NewFiles(&set)
	if err != nil {
		return nil, fmt.Errorf("protobuf: descriptorSet: %w (built with --include_imports?)", err)
	}
	return files.FindDescriptorByName, nil
}

// compile-time check that both resolvers fit finder
var _ finder = (*protoregistry.Files)(nil).FindDescriptorByName
