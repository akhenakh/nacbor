package cborcodec

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
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