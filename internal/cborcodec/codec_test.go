package cborcodec

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
)

// TestRoundTripJSON asserts that a JSON document survives a JSON -> CBOR ->
// JSON round trip using the encoder/decoder modes configured to mirror the
// Bento `cbor` processor.
func TestRoundTripJSON(t *testing.T) {
	src := map[string]any{
		"name":   "ada",
		"age":    float64(37),
		"tags":   []any{"math", "cs"},
		"active": true,
		"ts":     "2024-01-02T03:04:05.678901234Z",
		"bytes":  "aGVsbG8=",
		"meta":   map[string]any{"n": float64(1)},
	}
	jsonIn, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}

	cborData, err := EncodeFromJSON(jsonIn)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Sanity check: strings must be encoded as CBOR byte strings (major type 2),
	// per the bento StringToByteString option. "ada" as a byte string is 0x43
	// (major type 2, length 3) followed by 'a','d','a'.
	if hex.EncodeToString([]byte("\x43ada")) != "43616461" {
		t.Fatalf("sanity hex mismatch")
	}
	if !bytes.Contains(cborData, []byte("\x43ada")) { // "name" value "ada" as bytestring
		t.Errorf("encoded CBOR %x does not contain string \"ada\" encoded as a byte string", cborData)
	}

	jsonOut, err := DecodeToJSON(cborData)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(jsonOut, &got); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}
	if got["name"] != "ada" {
		t.Errorf("name = %v, want ada", got["name"])
	}
	if got["bytes"] != "aGVsbG8=" {
		t.Errorf("bytes = %v, want aGVsbG8=", got["bytes"])
	}
	if got["active"] != true {
		t.Errorf("active = %v, want true", got["active"])
	}
}

// TestDecodeEmptyGoesRoundTrip ensures a round trip of integers and nested
// containers keeps numeric types intact (float64 throughout, matching what
// json.Unmarshal yields).
func TestDecodePreservesNumbers(t *testing.T) {
	in := []byte(`{"n":42,"f":3.14,"arr":[1,2,3]}`)
	cborData, err := EncodeFromJSON(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeToJSON(cborData)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["n"].(float64) != 42 {
		t.Errorf("n = %v, want 42", got["n"])
	}
	if got["f"].(float64) != 3.14 {
		t.Errorf("f = %v, want 3.14", got["f"])
	}
}

// TestDecodeFallsBackToJSON verifies that Decode surfaces plain JSON payloads
// stored alongside CBOR ones in the same NATS store, instead of erroring out.
func TestDecodeFallsBackToJSON(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want any
	}{
		{"object", []byte(`{"k":"v","n":1}`), map[string]any{"k": "v", "n": float64(1)}},
		{"array", []byte(`[1,2,3]`), []any{float64(1), float64(2), float64(3)}},
		{"number", []byte(`42`), float64(42)},
		{"string", []byte(`"hello"`), "hello"},
		{"bool", []byte(`true`), true},
		{"null", []byte(`null`), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Decode(tc.in)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Decode(%s) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

// TestDecodeFallsBackToRawString verifies that non-structured bytes (neither
// CBOR nor JSON) still render as a string rather than failing the command.
func TestDecodeFallsBackToRawString(t *testing.T) {
	in := []byte("plain text payload \xff\x00 not json or cbor")
	got, err := Decode(in)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if s, ok := got.(string); !ok || s != string(in) {
		t.Errorf("Decode = %#v, want string %q", got, string(in))
	}
}

func TestDecodeStrictRejectsJSON(t *testing.T) {
	if _, err := DecodeStrict([]byte(`{"k":"v"}`)); err == nil {
		t.Fatal("DecodeStrict accepted JSON input; expected CBOR-only error")
	}
}