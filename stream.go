package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/akhenakh/nacbor/internal/cborcodec"
	"github.com/akhenakh/nacbor/internal/natsconn"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Inspect JetStream streams and fetch raw stream messages",
	Long: `Direct operations on JetStream streams: list streams, show stream info,
fetch a message by sequence or the last message for a subject, and purge.

JetStream messages whose payloads are CBOR-encoded (as produced by Bento) are
decoded to JSON by default; pass --raw to emit raw bytes.`,
}

func init() {
	rootCmd.AddCommand(streamCmd)
	streamCmd.AddCommand(streamListCmd())
	streamCmd.AddCommand(streamInfoCmd())
	streamCmd.AddCommand(streamGetCmd())
	streamCmd.AddCommand(streamGetLastCmd())
	streamCmd.AddCommand(streamPurgeCmd())
}

func streamListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List JetStream stream names",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			iter := js.StreamNames(ctx)
			out := cmd.OutOrStdout()
			for name := range iter.Name() {
				fmt.Fprintln(out, name)
			}
			return iter.Err()
		},
	}
}

func streamInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <stream>",
		Short: "Show JetStream stream info as JSON",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			str, err := js.Stream(ctx, args[0])
			if err != nil {
				return fmt.Errorf("open stream %q: %w", args[0], err)
			}
			info, err := str.Info(ctx)
			if err != nil {
				return fmt.Errorf("stream info: %w", err)
			}
			return emitJSON(cmd, info)
		},
	}
}

func streamGetCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "get <stream> <sequence>",
		Short: "Fetch a stream message by sequence and decode CBOR to JSON",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			stream := args[0]
			seq, err := parseSeq(args[1])
			if err != nil {
				return err
			}
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			str, err := js.Stream(ctx, stream)
			if err != nil {
				return fmt.Errorf("open stream %q: %w", stream, err)
			}
			raw, err := str.GetMsg(ctx, seq)
			if err != nil {
				return fmt.Errorf("get seq %d: %w", seq, err)
			}
			return emitRawStreamMsg(cmd, raw)
		},
	}
	return c
}

func streamGetLastCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get-last <stream> <subject>",
		Short: "Fetch the latest stream message for a subject and decode CBOR",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			stream, subject := args[0], args[1]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			str, err := js.Stream(ctx, stream)
			if err != nil {
				return fmt.Errorf("open stream %q: %w", stream, err)
			}
			raw, err := str.GetLastMsgForSubject(ctx, subject)
			if err != nil {
				return fmt.Errorf("get last msg for %q: %w", subject, err)
			}
			return emitRawStreamMsg(cmd, raw)
		},
	}
}

func streamPurgeCmd() *cobra.Command {
	var subject string
	var keep uint64
	var seq uint64
	c := &cobra.Command{
		Use:   "purge <stream>",
		Short: "Purge messages from a stream (destructive)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			stream := args[0]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			str, err := js.Stream(ctx, stream)
			if err != nil {
				return fmt.Errorf("open stream %q: %w", stream, err)
			}
			opts := []jetstream.StreamPurgeOpt{}
			if subject != "" {
				opts = append(opts, jetstream.WithPurgeSubject(subject))
			}
			if keep > 0 {
				opts = append(opts, jetstream.WithPurgeKeep(keep))
			}
			if seq > 0 {
				opts = append(opts, jetstream.WithPurgeSequence(seq))
			}
			if err := str.Purge(ctx, opts...); err != nil {
				return fmt.Errorf("purge: %w", err)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "purged %s\n", stream)
			return nil
		},
	}
	c.Flags().StringVar(&subject, "subject", "", "only purge messages matching this subject")
	c.Flags().Uint64Var(&keep, "keep", 0, "keep the last N messages (0 = purge all)")
	c.Flags().Uint64Var(&seq, "sequence", 0, "only purge messages up to and including this sequence")
	return c
}

func emitRawStreamMsg(cmd *cobra.Command, raw *jetstream.RawStreamMsg) error {
	rawFlag, _ := cmd.Flags().GetBool("raw")
	pretty, _ := cmd.Flags().GetBool("pretty")
	out := cmd.OutOrStdout()
	if rawFlag {
		_, err := out.Write(raw.Data)
		return err
	}
	obj := map[string]any{
		"stream":   "",
		"subject":  raw.Subject,
		"sequence": raw.Sequence,
		"headers":  raw.Header,
		"stored":   raw.Time,
	}
	if len(raw.Data) > 0 {
		decoded, derr := decodeCBOROrHex(raw.Data)
		if derr != nil {
			return derr
		}
		obj["payload"] = decoded
	}
	return writeJSON(out, obj, pretty)
}

// decodeCBOROrHex decodes CBOR, falling back to a raw string if decoding fails
// (so non-CBOR stream messages still render).
func decodeCBOROrHex(data []byte) (any, error) {
	v, err := cborcodec.Decode(data)
	if err != nil {
		return string(data), nil
	}
	return v, nil
}

func parseSeq(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid sequence %q: %w", s, err)
	}
	return n, nil
}

// guard against accidental unused-import churn; stderr helper if needed later.
var _ = os.Stderr