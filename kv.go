package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/akhenakh/nacbor/internal/cborcodec"
	"github.com/akhenakh/nacbor/internal/natsconn"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

var kvCmd = &cobra.Command{
	Use:   "kv",
	Short: "Operate on NATS KV buckets with CBOR-encoded values",
	Long: `Inspect and mutate NATS Key/Value buckets whose values are CBOR-encoded
(as produced by the Bento ` + "`cbor`" + ` processor).

Values are decoded to JSON on read and encoded from JSON on write, unless
--raw is given.
`,
}

func init() {
	rootCmd.AddCommand(kvCmd)
	addKVSubcommands(kvCmd)
}

func addKVSubcommands(parent *cobra.Command) {
	parent.AddCommand(kvGetCmd())
	parent.AddCommand(kvPutCmd())
	parent.AddCommand(kvCreateCmd())
	parent.AddCommand(kvUpdateCmd())
	parent.AddCommand(kvDeleteCmd())
	parent.AddCommand(kvPurgeCmd())
	parent.AddCommand(kvKeysCmd())
	parent.AddCommand(kvListCmd())
	parent.AddCommand(kvHistoryCmd())
	parent.AddCommand(kvWatchCmd())
	parent.AddCommand(kvStatusCmd())
	parent.AddCommand(kvBucketsCmd())
}

// kvGet: nacbor kv get <bucket> <key> [--revision N]
func kvGetCmd() *cobra.Command {
	var revision uint64
	c := &cobra.Command{
		Use:   "get <bucket> <key>",
		Short: "Get a value from a KV bucket and decode CBOR to JSON",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, key := args[0], args[1]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()

			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}

			var entry jetstream.KeyValueEntry
			if revision > 0 {
				entry, err = kv.GetRevision(ctx, key, revision)
			} else {
				entry, err = kv.Get(ctx, key)
			}
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrBucketNotFound) {
					return fmt.Errorf("key %q not found in bucket %q", key, bucket)
				}
				return fmt.Errorf("get %q: %w", key, err)
			}

			return emitPayload(cmd, entry.Value())
		},
	}
	c.Flags().Uint64Var(&revision, "revision", 0, "fetch a specific revision (0 = latest)")
	return c
}

// kvPut: nacbor kv put <bucket> <key> [file|-]
func kvPutCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "put <bucket> <key> [payload|-]",
		Short: "Encode JSON (or raw CBOR) to a KV key",
		Long: `Write a value to a KV bucket. By default the input is JSON from a positional
argument, stdin (-), or a file, encoded to CBOR using the same options as the
Bento ` + "`from_json`" + ` operator. With --raw, the bytes are stored verbatim.`,
		Args: cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, key := args[0], args[1]
			raw, _ := cmd.Flags().GetBool("raw")
			data, err := readInput(cmd, args, 2)
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

			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()

			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			rev, err := kv.Put(ctx, key, payload)
			if err != nil {
				return fmt.Errorf("put %q: %w", key, err)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "%s %s revision %d\n", bucket, key, rev)
			return nil
		},
	}
	return c
}

// kvCreate: only create if absent
func kvCreateCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "create <bucket> <key> [payload|-]",
		Short: "Create a key only if it does not already exist",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, key := args[0], args[1]
			raw, _ := cmd.Flags().GetBool("raw")
			data, err := readInput(cmd, args, 2)
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
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			rev, err := kv.Create(ctx, key, payload)
			if err != nil {
				return fmt.Errorf("create %q: %w", key, err)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "%s %s created revision %d\n", bucket, key, rev)
			return nil
		},
	}
	return c
}

// kvUpdate: update with optimistic concurrency on a revision
func kvUpdateCmd() *cobra.Command {
	var revision uint64
	c := &cobra.Command{
		Use:   "update <bucket> <key> [payload|-]",
		Short: "Update a key only if the latest revision matches --revision",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, key := args[0], args[1]
			raw, _ := cmd.Flags().GetBool("raw")
			data, err := readInput(cmd, args, 2)
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
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			rev, err := kv.Update(ctx, key, payload, revision)
			if err != nil {
				return fmt.Errorf("update %q: %w", key, err)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "%s %s updated revision %d\n", bucket, key, rev)
			return nil
		},
	}
	c.Flags().Uint64Var(&revision, "revision", 0, "expected latest revision (required)")
	_ = c.MarkFlagRequired("revision")
	return c
}

func kvDeleteCmd() *cobra.Command {
	var purge bool
	var revision uint64
	c := &cobra.Command{
		Use:   "delete <bucket> <key>",
		Short: "Delete a key (place a delete marker) or purge all revisions with --purge",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, key := args[0], args[1]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			if revision > 0 {
				if purge {
					return kv.Purge(ctx, key, jetstream.LastRevision(revision))
				}
				return kv.Delete(ctx, key, jetstream.LastRevision(revision))
			}
			if purge {
				return kv.Purge(ctx, key)
			}
			return kv.Delete(ctx, key)
		},
	}
	c.Flags().BoolVar(&purge, "purge", false, "purge all previous revisions (destructive)")
	c.Flags().Uint64Var(&revision, "revision", 0, "only delete/purge if latest revision matches")
	return c
}

func kvPurgeCmd() *cobra.Command {
	var olderThan time.Duration
	c := &cobra.Command{
		Use:   "purge-deletes <bucket>",
		Short: "Remove all delete markers from the bucket",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			if olderThan > 0 {
				return kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(olderThan))
			}
			return kv.PurgeDeletes(ctx)
		},
	}
	c.Flags().DurationVar(&olderThan, "older-than", 0, "only purge delete markers older than this duration")
	return c
}

func kvKeysCmd() *cobra.Command {
	var filters []string
	c := &cobra.Command{
		Use:   "keys <bucket>",
		Short: "List keys in a KV bucket",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			var l jetstream.KeyLister
			if len(filters) > 0 {
				l, err = kv.ListKeysFiltered(ctx, filters...)
			} else {
				l, err = kv.ListKeys(ctx)
			}
			if err != nil {
				return fmt.Errorf("list keys: %w", err)
			}
			defer l.Stop()
			out := cmd.OutOrStdout()
			for k := range l.Keys() {
				fmt.Fprintln(out, k)
			}
			return nil
		},
	}
	c.Flags().StringArrayVar(&filters, "filter", nil, "subject filter (repeatable)")
	return c
}

// kvList: dump every key/value (decoded) as JSON lines.
func kvListCmd() *cobra.Command {
	var filters []string
	c := &cobra.Command{
		Use:   "list <bucket>",
		Short: "List all key/value pairs (decoded CBOR) as JSON lines",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			raw, _ := cmd.Flags().GetBool("raw")
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			var w jetstream.KeyWatcher
			if len(filters) > 0 {
				w, err = kv.WatchFiltered(ctx, filters, jetstream.IgnoreDeletes())
			} else {
				w, err = kv.WatchAll(ctx, jetstream.IgnoreDeletes())
			}
			if err != nil {
				return fmt.Errorf("watch: %w", err)
			}
			defer w.Stop()
			out := cmd.OutOrStdout()
			for entry := range w.Updates() {
				if entry == nil {
					continue
				}
				if err := writeKVEntry(out, entry, raw); err != nil {
					return err
				}
			}
			return nil
		},
	}
	c.Flags().StringArrayVar(&filters, "filter", nil, "subject filter (repeatable)")
	return c
}

func kvHistoryCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "history <bucket> <key>",
		Short: "Show all historical revisions for a key (decoded)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, key := args[0], args[1]
			raw, _ := cmd.Flags().GetBool("raw")
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			entries, err := kv.History(ctx, key)
			if err != nil {
				return fmt.Errorf("history %q: %w", key, err)
			}
			out := cmd.OutOrStdout()
			for _, e := range entries {
				if err := writeKVEntry(out, e, raw); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return c
}

func kvWatchCmd() *cobra.Command {
	var (
		updatesOnly   bool
		includeHistory bool
		ignoreDeletes  bool
		metaOnly       bool
	)
	c := &cobra.Command{
		Use:   "watch <bucket> [key-filter]",
		Short: "Watch a KV bucket live, decoding CBOR values as JSON lines",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			raw, _ := cmd.Flags().GetBool("raw")
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			var opts []jetstream.WatchOpt
			if updatesOnly {
				opts = append(opts, jetstream.UpdatesOnly())
			}
			if includeHistory {
				opts = append(opts, jetstream.IncludeHistory())
			}
			if ignoreDeletes {
				opts = append(opts, jetstream.IgnoreDeletes())
			}
			if metaOnly {
				opts = append(opts, jetstream.MetaOnly())
			}
			var w jetstream.KeyWatcher
			if len(args) == 2 {
				w, err = kv.Watch(ctx, args[1], opts...)
			} else {
				w, err = kv.WatchAll(ctx, opts...)
			}
			if err != nil {
				return fmt.Errorf("watch: %w", err)
			}
			defer w.Stop()
			out := cmd.OutOrStdout()
			for entry := range w.Updates() {
				if entry == nil {
					continue
				}
				if err := writeKVEntry(out, entry, raw); err != nil {
					return err
				}
			}
			return nil
		},
	}
	c.Flags().BoolVar(&updatesOnly, "updates-only", false, "only deliver new updates (no initial snapshot)")
	c.Flags().BoolVar(&includeHistory, "history", false, "deliver all historical values first")
	c.Flags().BoolVar(&ignoreDeletes, "ignore-deletes", false, "do not emit delete markers")
	c.Flags().BoolVar(&metaOnly, "meta-only", false, "retrieve only metadata, not values")
	return c
}

func kvStatusCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "status <bucket>",
		Short: "Show KV bucket status/config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, bucket)
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", bucket, err)
			}
			status, err := kv.Status(ctx)
			if err != nil {
				return fmt.Errorf("status: %w", err)
			}
			return emitJSON(cmd, kvStatusView{status})
		},
	}
	return c
}

func kvBucketsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "buckets",
		Short: "List all KV bucket names",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := natsconn.JS(natsconn.FromViper())
			if err != nil {
				return err
			}
			defer nc.Drain()
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			names := js.KeyValueStoreNames(ctx)
			out := cmd.OutOrStdout()
			for name := range names.Name() {
				fmt.Fprintln(out, name)
			}
			return names.Error()
		},
	}
	return c
}

// kvStatusView wraps KeyValueStatus to expose commonly used fields as JSON.
type kvStatusView struct {
	inner jetstream.KeyValueStatus
}

func (s kvStatusView) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"bucket":        s.inner.Bucket(),
		"values":        s.inner.Values(),
		"bytes":         s.inner.Bytes(),
		"history":       s.inner.History(),
		"ttl":           s.inner.TTL(),
		"backing_store": s.inner.BackingStore(),
		"compressed":    s.inner.IsCompressed(),
	})
}

// writeKVEntry prints a single KV entry as a JSON line.
func writeKVEntry(w io.Writer, e jetstream.KeyValueEntry, raw bool) error {
	var value any
	if e.Operation() == jetstream.KeyValuePut {
		if raw {
			value = e.Value()
		} else {
			decoded, err := cborcodec.Decode(e.Value())
			if err != nil {
				return err
			}
			value = decoded
		}
	}
	return writeJSON(w, map[string]any{
		"bucket":    e.Bucket(),
		"key":       e.Key(),
		"revision":  e.Revision(),
		"created":   e.Created(),
		"delta":     e.Delta(),
		"operation": e.Operation().String(),
		"value":     value,
	}, false)
}

// readInput resolves the payload for put/create/update from a positional
// argument (index), "-" (stdin), or a file path. If the argument is absent it
// reads all of stdin.
func readInput(cmd *cobra.Command, args []string, idx int) ([]byte, error) {
	if idx < len(args) {
		src := args[idx]
		if src == "-" {
			return io.ReadAll(cmd.InOrStdin())
		}
		return os.ReadFile(src)
	}
	return io.ReadAll(cmd.InOrStdin())
}