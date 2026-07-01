// Package cborcodec mirrors the encoder/decoder options used by the Bento
// `cbor` processor (github.com/fxamacker/cbor/v2) so that values produced and
// consumed by pipelines built with that plugin round-trip cleanly through
// `nacbor`.
package cborcodec

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

// EncMode is the CBOR encoder mode matching the bento `from_json` operator.
var EncMode cbor.EncMode

// DecMode is the CBOR decoder mode matching the bento `to_json` operator.
var DecMode cbor.DecMode

func init() {
	var err error

	encOpts := cbor.PreferredUnsortedEncOptions()
	encOpts.ByteSliceLaterFormat = cbor.ByteSliceLaterFormatBase64
	encOpts.String = cbor.StringToByteString
	encOpts.ByteArray = cbor.ByteArrayToArray
	encOpts.Time = cbor.TimeRFC3339NanoUTC

	if EncMode, err = encOpts.EncMode(); err != nil {
		panic(fmt.Errorf("cborcodec: failed to create encoder mode: %w", err))
	}

	decOpts := cbor.DecOptions{
		MapKeyByteString:       cbor.MapKeyByteStringAllowed,
		DefaultMapType:         reflect.TypeOf(map[string]any{}),
		DefaultByteStringType:  reflect.TypeOf(""),
		ByteStringToString:     cbor.ByteStringToStringAllowed,
		IndefLength:            cbor.IndefLengthAllowed,
	}

	if DecMode, err = decOpts.DecMode(); err != nil {
		panic(fmt.Errorf("cborcodec: failed to create decoder mode: %w", err))
	}
}

// Decode unmarshals CBOR bytes into a generic Go value (the same shape Bento's
// `to_json` operator produces when it calls SetStructured).
//
// NATS stores are not always populated by Bento pipelines: a value may be
// plain JSON or other bytes. To keep nacbor usable against mixed-content
// buckets/streams, when CBOR decoding fails Decode falls back to parsing the
// bytes as JSON, and finally to the raw bytes as a string. An error is only
// returned if the input is neither valid CBOR nor valid JSON.
func Decode(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var v any
	if err := DecMode.Unmarshal(data, &v); err == nil {
		return v, nil
	}

	// Not CBOR — try JSON next. json.Valid avoids turning arbitrary garbage
	// into a successful (but meaningless) string fallback, while still
	// accepting whitespace-only bodies that json.Unmarshal tolerates.
	if json.Valid(data) {
		var jv any
		if err := json.Unmarshal(data, &jv); err == nil {
			return jv, nil
		}
	}

	// Last resort: present the bytes verbatim as a string so non-structured
	// payloads (text, protobuf, opaque blobs) still render instead of
	// failing the whole command.
	return string(data), nil
}

// DecodeStrict unmarshals CBOR bytes only; it returns an error if the input
// is not valid CBOR. Use this when the caller must guarantee CBOR semantics
// (e.g. round-tripping values back into a store without re-encoding).
func DecodeStrict(data []byte) (any, error) {
	var v any
	if err := DecMode.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("decode CBOR: %w", err)
	}
	return v, nil
}

// DecodeToJSON runs Decode then re-encodes the result as JSON. This is what
// callers usually want for display since the bento decoder yields types that
// json.Marshal can render directly.
func DecodeToJSON(data []byte) ([]byte, error) {
	v, err := Decode(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// MarshalJSONUnmarshalCBOR is the inverse of Decode: it parses JSON (using
// standard json.Unmarshal so that numbers become float64 rather than
// json.Number, which the CBOR encoder would otherwise emit as raw strings)
// and then encodes the resulting value to CBOR, mirroring the bento
// `from_json` operator.
func EncodeFromJSON(jsonData []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(jsonData, &v); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	out, err := EncMode.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode JSON to CBOR: %w", err)
	}
	return out, nil
}