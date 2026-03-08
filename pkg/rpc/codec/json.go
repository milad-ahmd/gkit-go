// Package codec provides a JSON codec for gRPC, enabling services to use
// plain Go structs (with JSON tags) instead of protobuf-generated types.
//
// Register it once at startup, before creating any gRPC connections:
//
//	import "github.com/miladhzz/gkit/pkg/rpc/codec"
//	func init() { codec.Register() }
package codec

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

const (
	// NameJSON is the content-subtype for the JSON codec.
	NameJSON = "json"
	// NameProto is the default gRPC codec name — we override it so plain Go
	// structs work out-of-the-box without protobuf tooling.
	NameProto = "proto"
)

// jsonCodec implements encoding.Codec using the standard library's encoding/json.
type jsonCodec struct{ name string }

func (c jsonCodec) Name() string              { return c.name }
func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Register installs the JSON codec into the gRPC codec registry under both
// "json" and "proto". Overriding "proto" lets standard gRPC clients and servers
// use plain Go structs with JSON tags without a protobuf dependency.
//
// In production services that mix protobuf and JSON, use per-call
// grpc.ForceCodec(codec.JSON{name:"json"}) instead of overriding "proto".
func Register() {
	encoding.RegisterCodec(jsonCodec{name: NameJSON})
	encoding.RegisterCodec(jsonCodec{name: NameProto})
}
