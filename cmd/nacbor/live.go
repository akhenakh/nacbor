package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/akhenakh/nacbor/internal/cborcodec"
	"github.com/akhenakh/nacbor/internal/natsconn"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

// liveCmd: nacbor live <stream>
var liveCmd = &cobra.Command{
	Use:   "live <stream>",
	Short: "Follow a JetStream stream live, decoding CBOR payloads as JSON",
	Long: `Continuously consume messages from a JetStream stream via an ordered pull
consumer and decode each CBOR payload to JSON. By default only new messages
are delivered (--start new); use --start all|last to backfill history first.

Press Ctrl-C to stop. The consumer is ephemeral (ordered) and is cleaned up
automatically.`,
	Args: cobra.ExactArgs(1),
	RunE: runLive,
}

func init() {
	rootCmd.AddCommand(liveCmd)
	liveCmd.Flags().StringArrayP("filter", "f", nil, "filter subjects (repeatable); empty = all stream subjects")
	liveCmd.Flags().StringP("start", "", "new", "start position: all|last|new|seq=<n>|time=<RFC3339>")
	liveCmd.Flags().Bool("ack", false, "acknowledge each message after processing")
	liveCmd.Flags().Bool("no-decode", false, "emit raw payload bytes instead of decoding CBOR to JSON")
	liveCmd.Flags().Bool("headers", false, "include message headers in output")
	liveCmd.Flags().Bool("metadata", true, "include JetStream metadata (stream/consumer/seq) in output")
	liveCmd.Flags().Uint64("batch", 100, "internal pull batch size")
}

func runLive(cmd *cobra.Command, args []string) error {
	stream := args[0]
	filters, _ := cmd.Flags().GetStringArray("filter")
	startStr, _ := cmd.Flags().GetString("start")
	ack, _ := cmd.Flags().GetBool("ack")
	noDecode, _ := cmd.Flags().GetBool("no-decode")
	showHeaders, _ := cmd.Flags().GetBool("headers")
	showMeta, _ := cmd.Flags().GetBool("metadata")
	batch, _ := cmd.Flags().GetUint64("batch")

	ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	nc, js, err := natsconn.JS(natsconn.FromViper())
	if err != nil {
		return err
	}
	defer nc.Drain()

	deliver, optStart, startTime, err := parseStart(startStr)
	if err != nil {
		return err
	}

	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy: deliver,
		OptStartSeq:   optStart,
		OptStartTime:  startTime,
	}
	if len(filters) > 0 {
		cfg.FilterSubjects = filters
	}

	cons, err := js.OrderedConsumer(ctx, stream, cfg)
	if err != nil {
		return fmt.Errorf("create ordered consumer on stream %q: %w", stream, err)
	}

	out := cmd.OutOrStdout()
	errCh := make(chan error, 1)
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()

	handler := func(msg jetstream.Msg) {
		if msg == nil {
			return
		}
		if err := writeJetstreamMsg(out, msg, noDecode, showHeaders, showMeta, ack); err != nil {
			select {
			case errCh <- err:
			default:
			}
			ccancel()
		}
	}

	copts := []jetstream.PullConsumeOpt{
		jetstream.PullMaxMessages(int(batch)),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			if err == nil || err == jetstream.ErrNoMessages {
				return
			}
			fmt.Fprintf(os.Stderr, "nacbor: consume error: %v\n", err)
		}),
	}

	cc, err := cons.Consume(handler, copts...)
	if err != nil {
		return fmt.Errorf("start consuming: %w", err)
	}
	defer cc.Stop()

	select {
	case <-cctx.Done():
	case err := <-errCh:
		cancel()
		return err
	case <-ctx.Done():
	}
	return nil
}

func writeJetstreamMsg(w io.Writer, msg jetstream.Msg, noDecode, showHeaders, showMeta, ack bool) error {
	var value any
	data := msg.Data()
	if noDecode || len(data) == 0 {
		value = data
	} else {
		decoded, err := cborcodec.Decode(data)
		if err != nil {
			return fmt.Errorf("decode CBOR on subject %q: %w", msg.Subject(), err)
		}
		value = decoded
	}

	obj := map[string]any{
		"subject": msg.Subject(),
		"payload": value,
	}

	if showMeta {
		if meta, err := msg.Metadata(); err == nil {
			obj["stream"] = meta.Stream
			obj["consumer"] = meta.Consumer
			obj["stream_seq"] = meta.Sequence.Stream
			obj["consumer_seq"] = meta.Sequence.Consumer
			obj["delivered"] = meta.NumDelivered
			obj["pending"] = meta.NumPending
			obj["timestamp"] = meta.Timestamp
		}
	}
	if showHeaders && len(msg.Headers()) > 0 {
		obj["headers"] = nats.Header(msg.Headers())
	}

	if err := writeJSON(w, obj, false); err != nil {
		return err
	}

	if ack {
		if err := msg.Ack(); err != nil {
			return fmt.Errorf("ack: %w", err)
		}
	}
	return nil
}

// parseStart converts the --start flag value into a DeliverPolicy plus, when
// applicable, an OptStartSeq or OptStartTime.
func parseStart(s string) (jetstream.DeliverPolicy, uint64, *time.Time, error) {
	switch s {
	case "", "all":
		return jetstream.DeliverAllPolicy, 0, nil, nil
	case "last":
		return jetstream.DeliverLastPolicy, 0, nil, nil
	case "new":
		return jetstream.DeliverNewPolicy, 0, nil, nil
	}
	if rest, ok := strings.CutPrefix(s, "seq="); ok {
		n, err := strconv.ParseUint(rest, 10, 64)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("--start seq=: %w", err)
		}
		return jetstream.DeliverByStartSequencePolicy, n, nil, nil
	}
	if rest, ok := strings.CutPrefix(s, "time="); ok {
		t, err := time.Parse(time.RFC3339, rest)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("--start time=: %w", err)
		}
		return jetstream.DeliverByStartTimePolicy, 0, &t, nil
	}
	return 0, 0, nil, fmt.Errorf("invalid --start %q (want all|last|new|seq=<n>|time=<RFC3339>)", s)
}