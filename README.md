# nacbor

`nacbor` is a CLI for inspecting and manipulating **CBOR-encoded** data stored in
NATS, NATS JetStream streams, and NATS KV buckets.

The encoder/decoder uses
[`github.com/fxamacker/cbor/v2`](https://github.com/fxamacker/cbor).

```text
 JSON  --nacbor put/ pub -->  CBOR  -->  NATS JetStream / KV
 CBOR  --nacbor get/ live-->  JSON  -->  your terminal / pipe
```

## Install

```bash
task build                    # produces ./bin/nacbor with version ldflags
# or
go install github.com/akhenakh/nacbor      # installs the `nacbor` binary to $GOBIN
# or
task install                   # same `go install`, with version ldflags
```

> The main package lives at the repository root (no `cmd/` indirection), so a
> plain `go install github.com/akhenakh/nacbor` is enough — there's no
> `/cmd/nacbor` suffix to remember.

Requires Go 1.26+. Verify against a running JetStream-enabled server:

```bash
nats-server -js &
nacbor kv buckets       # lists KV buckets visible to the connection
```

## Connection & auth

All connection flags are global, inherited by every subcommand, and bindable
to `NATSBOR_*` environment variables or a `~/.nacbor.yaml` config file.

| Flag | Env | Description |
| --- | --- | --- |
| `-s, --server` | `NATSBOR_SERVER` | NATS URL (default `nats://localhost:4222`) |
| `--nkey` | `NATSBOR_NKEY` | path to NKEY seed file |
| `--creds` | `NATSBOR_CREDS` | path to JWT+seed credentials file |
| `--user` / `--password` | `NATSBOR_USER` / `NATSBOR_PASSWORD` | username/password auth |
| `--token` | `NATSBOR_TOKEN` | bearer token |
| `--tls-cert` / `--tls-key` / `--tls-ca` | `NATSBOR_TLS_*` | mTLS / CA bundle |
| `--insecure` | `NATSBOR_INSECURE` | skip TLS verification |
| `--timeout` | `NATSBOR_TIMEOUT` | connection timeout (default `10s`) |

Auth methods are tried in this order: **NKEY seed → creds file → user/password
→ token**.

### Output flags (global)

- `--raw` — skip CBOR decode/encode; emit or store raw bytes. Lets `nacbor`
  sit in a pipe between other CBOR-aware tools.
- `-p, --pretty` — pretty-print JSON for single-record commands
  (`kv get`, `kv status`, `stream get`, `stream info`, …). Streaming commands
  (`kv list`, `kv watch`, `live`, `nats sub`) always emit NDJSON so they stay
  line-delimited for `jq`, `grep`, etc.

## CBOR codec

- **Encoder**: `PreferredUnsortedEncOptions` + `ByteSliceLaterFormatBase64`,
  `StringToByteString`, `ByteArrayToArray`, `TimeRFC3339NanoUTC`.
- **Decoder**: `MapKeyByteStringAllowed`, `DefaultMapType=map[string]any`,
  `DefaultByteStringType=string`, `ByteStringToStringAllowed`,
  `IndefLengthAllowed`.

## Quick tour

```text
nacbor kv     get | put | create | update | delete | purge-deletes |
              keys | list | history | watch | status | buckets
nacbor stream list | info | get | get-last | purge
nacbor live   <stream>                     # follow a JetStream stream
nacbor nats   pub | sub | req              # core NATS with CBOR envelopes
nacbor version
```

## Real usage examples

The outputs below were captured against a live `nats-server -js` with a KV
bucket `NACBORDEMO` and a stream `NACBORLOGS`. The payloads were produced by
`nacbor` itself (JSON in → CBOR out).
`cbor` processor would write.

### Set up the demo data

Create a KV bucket and write two device records plus a firmware manifest as
CBOR-encoded JSON:

```bash
$ nats kv add NACBORDEMO
Information for Key-Value Store Bucket NACBORDEMO created 2026-06-30 14:47:45
...

$ echo '{"device":"sensor-42","firmware":"1.8.2","rssi":-67,"online":true,"ts":"2026-06-30T18:45:12.332Z"}' \
    | nacbor kv put NACBORDEMO devices/sensor-42 -
NACBORDEMO devices/sensor-42 revision 1

$ echo '{"device":"sensor-7","firmware":"1.8.2","rssi":-81,"online":false,"ts":"2026-06-30T18:45:13.001Z"}' \
    | nacbor kv put NACBORDEMO devices/sensor-7 -
NACBORDEMO devices/sensor-7 revision 2

$ echo '{"version":"1.8.2","released":"2026-06-01","bundles":["fw","net","ble"]}' \
    | nacbor kv put NACBORDEMO firmware/1.8.2 -
NACBORDEMO firmware/1.8.2 revision 3
```

> Input can come from a positional argument, `-` (stdin), or a file path. JSON
> is encoded to CBOR unless `--raw` is given.

### `kv get` — read and decode a value

By default `nacbor` decodes the stored CBOR back to JSON:

```bash
$ nacbor kv get NACBORDEMO devices/sensor-42
{"device":"sensor-42","firmware":"1.8.2","online":true,"rssi":-67,"ts":"2026-06-30T18:45:12.332Z"}

$ nacbor kv get NACBORDEMO devices/sensor-42 -p
{
  "device": "sensor-42",
  "firmware": "1.8.2",
  "online": true,
  "rssi": -67,
  "ts": "2026-06-30T18:45:12.332Z"
}
```

Fetch a specific revision with `--revision`:

```bash
$ nacbor kv get NACBORDEMO devices/sensor-42 --revision 1
{"device":"sensor-42","firmware":"1.8.2","online":true,"rssi":-67,"ts":"2026-06-30T18:45:12.332Z"}
```

### `kv get --raw` — inspect the raw CBOR bytes

Pipe `--raw` into `xxd` / `file` to inspect the actual wire bytes (note the
`X` 0x58 length prefix and `H` 0x48 tags
layout, not random binary):

```bash
$ nacbor kv get NACBORDEMO devices/sensor-42 --raw | xxd | head -4
00000000: a542 7473 5818 3230 3236 2d30 362d 3330  .BtsX.2026-06-30
00000010: 5431 383a 3435 3a31 322e 3333 325a 4664  T18:45:12.332ZFd
00000020: 6576 6963 6549 7365 6e73 6f72 2d34 3248  eviceIsensor-42H
00000030: 6669 726d 7761 7265 4531 2e38 2e32 4472  firmwareE1.8.2Dr
```

### `kv keys`, `kv list`, `kv status`

```bash
$ nacbor kv keys NACBORDEMO
devices/sensor-42
devices/sensor-7
firmware/1.8.2

$ nacbor kv list NACBORDEMO
{"bucket":"NACBORDEMO","created":"2026-06-30T15:03:14.206840322-04:00","delta":3,"key":"devices/sensor-7","operation":"KeyValuePutOp","revision":2,"value":{"device":"sensor-7","firmware":"1.8.2","online":false,"rssi":-81,"ts":"2026-06-30T18:45:13.001Z"}}
{"bucket":"NACBORDEMO","created":"2026-06-30T15:03:14.211562386-04:00","delta":2,"key":"firmware/1.8.2","operation":"KeyValuePutOp","revision":3,"value":{"bundles":["fw","net","ble"],"released":"2026-06-01","version":"1.8.2"}}
{"bucket":"NACBORDEMO","created":"2026-06-30T15:03:54.272244176-04:00","delta":1,"key":"devices/sensor-42","operation":"KeyValuePutOp","revision":5,"value":{"device":"sensor-42","rssi":-59,"ts":"2026-06-30T19:05:00.0Z"}}

$ nacbor kv status NACBORDEMO -p
{
  "backing_store": "JetStream",
  "bucket": "NACBORDEMO",
  "bytes": 513,
  "compressed": false,
  "history": 10,
  "ttl": 0,
  "values": 5
}
```

### `kv watch` — stream updates as they happen

`kv watch` emits one JSON object per line as values change. Open one terminal
on the bucket and update a key from another:

```bash
# terminal 2
$ nacbor kv watch NACBORDEMO --updates-only
{"bucket":"NACBORDEMO","created":"2026-06-30T15:03:54.272244176-04:00","delta":0,"key":"devices/sensor-42","operation":"KeyValuePutOp","revision":5,"value":{"device":"sensor-42","rssi":-59,"ts":"2026-06-30T19:05:00.0Z"}}

# terminal 1
$ echo '{"device":"sensor-42","rssi":-59,"ts":"2026-06-30T19:05:00.0Z"}' \
    | nacbor kv put NACBORDEMO devices/sensor-42 -
NACBORDEMO devices/sensor-42 revision 5
```

Useful flags: `--history` (replay all revisions first), `--ignore-deletes`,
`--meta-only` (just metadata, no payload), `--updates-only` (skip initial
snapshot). Pass a key filter as the second argument
(`nacbor kv watch NACBORDEMO 'devices.*'`).

### `live` — follow a JetStream stream live

`live <stream>` continuously consumes messages via an **ordered pull consumer**
and decodes each CBOR payload to JSON on the fly. First, publish a few CBOR
messages to a stream backed by the `logs.>` subject:

```bash
$ nats stream add NACBORLOGS --subjects 'logs.>' --storage file --defaults

$ for i in 1 2 3; do
    echo "{\"level\":\"info\",\"msg\":\"boot\",\"seq\":$i,\"ts\":\"2026-06-30T18:5$i:0$i.0Z\"}" \
      | nacbor nats pub logs.app.$i -
done
published 54 bytes to logs.app.1
published 54 bytes to logs.app.2
published 54 bytes to logs.app.3
```

Replay the whole stream from the beginning with `--start all`, acknowledging
each message as you go:

```bash
$ nacbor live NACBORLOGS --start all --ack
{"consumer":"URa76L9s2Xo4tYNgkkKGw6_1","consumer_seq":1,"delivered":1,"payload":{"level":"info","msg":"boot","seq":1,"ts":"2026-06-30T18:51:01.0Z"},"pending":2,"stream":"NACBORLOGS","stream_seq":1,"subject":"logs.app.1","timestamp":"2026-06-30T15:03:29.153157172-04:00"}
{"consumer":"URa76L9s2Xo4tYNgkkKGw6_1","consumer_seq":2,"delivered":1,"payload":{"level":"info","msg":"boot","seq":2,"ts":"2026-06-30T18:52:02.0Z"},"pending":1,"stream":"NACBORLOGS","stream_seq":2,"subject":"logs.app.2","timestamp":"2026-06-30T15:03:29.157080583-04:00"}
{"consumer":"URa76L9s2Xo4tYNgkkKGw6_1","consumer_seq":3,"delivered":1,"payload":{"level":"info","msg":"boot","seq":3,"ts":"2026-06-30T18:53:03.0Z"},"pending":0,"stream":"NACBORLOGS","stream_seq":3,"subject":"logs.app.3","timestamp":"2026-06-30T15:03:29.16082751-04:00"}
```

Now **follow new messages in real time** — open `live` in one terminal with
`--start new` and publish from another:

```bash
# terminal 1
$ nacbor live NACBORLOGS --start new

# terminal 2
$ echo '{"level":"warn","msg":"disk almost full","seq":4,"ts":"2026-06-30T19:01:00.0Z"}' \
    | nacbor nats pub logs.app.4 -
$ echo '{"level":"error","msg":"oom","seq":5,"ts":"2026-06-30T19:02:00.0Z"}' \
    | nacbor nats pub logs.app.5 -

# terminal 1 emits, as the messages arrive:
{"consumer":"mrhZTPfdcRt70IrasfjuAT_1","consumer_seq":1,"delivered":1,"payload":{"level":"warn","msg":"disk almost full","seq":4,"ts":"2026-06-30T19:01:00.0Z"},"pending":0,"stream":"NACBORLOGS","stream_seq":4,"subject":"logs.app.4","timestamp":"2026-06-30T15:03:40.244913264-04:00"}
{"consumer":"mrhZTPfdcRt70IrasfjuAT_1","consumer_seq":2,"delivered":1,"payload":{"level":"error","msg":"oom","seq":5,"ts":"2026-06-30T19:02:00.0Z"},"pending":0,"stream":"NACBORLOGS","stream_seq":5,"subject":"logs.app.5","timestamp":"2026-06-30T15:03:40.249131965-04:00"}
```

Each line is a self-contained JSON object, ready for `jq`:

```bash
$ nacbor live NACBORLOGS --start new | jq -c '{seq:.stream_seq, lvl:.payload.level, msg:.payload.msg}'
{"seq":4,"lvl":"warn","msg":"disk almost full"}
{"seq":5,"lvl":"error","msg":"oom"}
```

Press `Ctrl-C` to stop; the ordered consumer is ephemeral and cleans itself
up automatically.

#### `--start` reference

| Value | Meaning |
| --- | --- |
| `all` | replay from the beginning of the stream |
| `last` | start at the last message |
| `new` | only messages published after `live` starts (default) |
| `seq=<N>` | start at stream sequence `N` |
| `time=<RFC3339>` | start at the given wall-clock time |

Other `live` flags: `-f/--filter` (repeatable subject filters), `--headers`
(include NATS headers), `--metadata=false` (drop the `stream`/`consumer`/`seq`
envelope), `--no-decode` (emit raw payload bytes instead of decoding CBOR),
`--ack` (ack each message).

### `stream get` / `get-last` — fetch individual stream messages

```bash
$ nacbor stream get NACBORLOGS 4 -p
{
  "headers": {
    "Nats-Sequence": ["4"],
    "Nats-Stream": ["NACBORLOGS"],
    "Nats-Subject": ["logs.app.4"],
    "Nats-Time-Stamp": ["2026-06-30T19:03:40.244913264Z"]
  },
  "payload": {
    "level": "warn",
    "msg": "disk almost full",
    "seq": 4,
    "ts": "2026-06-30T19:01:00.0Z"
  },
  "sequence": 4,
  "stored": "2026-06-30T19:03:40.244913264Z",
  "stream": "",
  "subject": "logs.app.4"
}

$ nacbor stream get-last NACBORLOGS logs.app.5
{"headers":{"Nats-Sequence":["5"],"Nats-Stream":["NACBORLOGS"],"Nats-Subject":["logs.app.5"],"Nats-Time-Stamp":["2026-06-30T19:03:40.249131965Z"]},"payload":{"level":"error","msg":"oom","seq":5,"ts":"2026-06-30T19:02:00.0Z"},"sequence":5,"stored":"2026-06-30T19:03:40.249131965Z","stream":"","subject":"logs.app.5"}
```

### Core NATS: `pub`, `sub`, `req`

These operate on plain NATS subjects (no JetStream) and apply the same
CBOR/JSON translation:

```bash
$ nats sub req.echo        # in another shell, reply with CBOR
$ echo '{"q":"hello"}' | nacbor nats req req.echo -
{"pong":"ok","echo":{"q":"hello"}}

$ nacbor nats sub logs.app.> -n 1
{"payload":{"level":"info","msg":"boot","seq":1,"ts":"2026-06-30T18:51:01.0Z"},"reply":"","subject":"logs.app.1"}
```

`-n` / `--count` unsubscribes after N messages; omit it to subscribe forever
(until `Ctrl-C`).

## Shell completion

```bash
nacbor completion bash > /etc/bash_completion.d/nacbor   # or zsh / fish / powershell
```

## Project layout

```text
*.go (repo root)    package main — cobra CLI (root + kv / live / stream / nats / version)
internal/cborcodec/ CBOR enc/dec
internal/natsconn/  NATS connection helper (NKEY, creds, userpass, token, TLS)
```

There is no `cmd/` indirection — `go install github.com/akhenakh/nacbor` builds
`nacbor` directly from the root package.

## Build / test / lint

```bash
task build      # bin/nacbor, with version/commit/date ldflags
task test       # go test ./...
task vet        # go vet ./...
task check      # build + test + vet (CI-style)
```

Tasks are defined in `Taskfile.yml` ([taskfile.dev](https://taskfile.dev)); run
`task --list` to see them all. Plain `go build .` / `go test ./...` /
`go vet ./...` work too.

## License

MIT 
