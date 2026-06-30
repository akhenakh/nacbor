package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/akhenakh/nacbor/internal/cborcodec"
	"github.com/spf13/cobra"
)

// emitPayload writes a single CBOR payload to the command's stdout, decoding to
// JSON by default. When --raw is set, the raw bytes are written (unmodified)
// so the tool can be piped into other CBOR-aware tools.
func emitPayload(cmd *cobra.Command, data []byte) error {
	raw, _ := cmd.Flags().GetBool("raw")
	pretty, _ := cmd.Flags().GetBool("pretty")
	out := cmd.OutOrStdout()

	if raw {
		_, err := out.Write(data)
		return err
	}

	v, err := cborcodec.Decode(data)
	if err != nil {
		return err
	}
	return writeJSON(out, v, pretty)
}

// emitJSON marshals an already-native Go value as JSON.
func emitJSON(cmd *cobra.Command, v any) error {
	pretty, _ := cmd.Flags().GetBool("pretty")
	return writeJSON(cmd.OutOrStdout(), v, pretty)
}

func writeJSON(w io.Writer, v any, pretty bool) error {
	var (
		b   []byte
		err error
	)
	if pretty {
		b, err = json.MarshalIndent(v, "", "  ")
	} else {
		b, err = json.Marshal(v)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}