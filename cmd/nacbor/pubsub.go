package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/akhenakh/nacbor/internal/cborcodec"
	"github.com/akhenakh/nacbor/internal/natsconn"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

var pubsubCmd = &cobra.Command{
	Use:   "nats",
	Short: "Core NATS publish/subscribe with CBOR encode/decode",
	Long: `Core NATS (non-JetStream) operations. ` + "`pub`" + ` encodes JSON to CBOR before
publishing; ` + "`sub`" + ` decodes CBOR on receipt. Use --raw to skip encoding/decoding.`,
}

func init() {
	rootCmd.AddCommand(pubsubCmd)
	pubsubCmd.AddCommand(pubCmd())
	pubsubCmd.AddCommand(reqCmd())
	pubsubCmd.AddCommand(subCmd())
}

func pubCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "pub <subject> [payload|-]",
		Short: "Publish CBOR-encoded JSON to a subject",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			subject := args[0]
			raw, _ := cmd.Flags().GetBool("raw")
			data, err := readInput(cmd, args, 1)
			if err != nil {
				return err
			}
			var payload []byte
			if raw {
				payload = data
			} else {
				payload, err = cborcodec.EncodeFromJSON(data)
				if err != nil {
					return err
				}
			}
			nc, err := natsconn.Connect(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			if err := nc.Publish(subject, payload); err != nil {
				return fmt.Errorf("publish: %w", err)
			}
			if err := nc.Flush(); err != nil {
				return fmt.Errorf("flush: %w", err)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "published %d bytes to %s\n", len(payload), subject)
			return nil
		},
	}
	return c
}

func reqCmd() *cobra.Command {
	var timeout time.Duration
	c := &cobra.Command{
		Use:   "req <subject> [payload|-]",
		Short: "Send a request to a subject and decode the CBOR reply",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			subject := args[0]
			raw, _ := cmd.Flags().GetBool("raw")
			data, err := readInput(cmd, args, 1)
			if err != nil {
				return err
			}
			var payload []byte
			if raw {
				payload = data
			} else {
				payload, err = cborcodec.EncodeFromJSON(data)
				if err != nil {
					return err
				}
			}
			nc, err := natsconn.Connect(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx, subject, payload)
			if err != nil {
				return fmt.Errorf("request: %w", err)
			}
			return emitPayload(cmd, msg.Data)
		},
	}
	c.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "request timeout")
	return c
}

func subCmd() *cobra.Command {
	var (
		queue string
		count int
	)
	c := &cobra.Command{
		Use:   "sub <subject>",
		Short: "Subscribe to a subject and decode CBOR payloads as JSON lines",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			subject := args[0]
			raw, _ := cmd.Flags().GetBool("raw")
			nc, err := natsconn.Connect(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()

			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			out := cmd.OutOrStdout()
			var sub *nats.Subscription
			var received int64
			handler := func(m *nats.Msg) {
				obj := map[string]any{
					"subject":  m.Subject,
					"reply":    m.Reply,
					"payload": cborOrRaw(m.Data, raw),
				}
				if len(m.Header) > 0 {
					obj["headers"] = m.Header
				}
				_ = writeJSON(out, obj, false)
				if count > 0 {
					if atomic.AddInt64(&received, 1) >= int64(count) {
						cancel()
					}
				}
			}

			if queue != "" {
				sub, err = nc.QueueSubscribe(subject, queue, handler)
			} else {
				sub, err = nc.Subscribe(subject, handler)
			}
			if err != nil {
				return fmt.Errorf("subscribe: %w", err)
			}
			defer sub.Unsubscribe()

			<-ctx.Done()
			return nil
		},
	}
	c.Flags().StringVar(&queue, "queue", "", "queue group name")
	c.Flags().IntVarP(&count, "count", "n", 0, "unsubscribe after N messages (0 = forever)")
	return c
}

func cborOrRaw(data []byte, raw bool) any {
	if raw || len(data) == 0 {
		return data
	}
	v, err := cborcodec.Decode(data)
	if err != nil {
		return string(data)
	}
	return v
}