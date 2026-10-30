# s3-interop

`script/s3-interop` checks `s3_out` against a real MinIO and a real Ceph RGW, or, in
[AWS mode](#aws-mode), a real S3 bucket. It runs the legs in [What it runs](#what-it-runs) against
each store, fetches every object with the AWS CLI, tests each with the stock `gzip` and `zstd`, and
prints one row per leg and store. [Recorded run](#recorded-run) records what a run showed.

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/kafka-interop` and `script/splunk-interop`. It isn't part of `script/cibuild`, and no test
depends on it running. A local run takes about five minutes once the images are built. Ceph takes
under a minute of that to start.

## What it runs

`compose.yaml` starts the stores under the compose project `s3-interop`, on the network
`s3-interop-net`, with no host ports. Every credential is a fixed throwaway for a container that
publishes nothing. The script starts each leg with `docker run` on that network, once per store, so
the same config serves every store.

| Container | Image | Role |
|---|---|---|
| `minio` | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, pinned by digest | MinIO with its built-in KMS on, so SSE-S3 works |
| `rgw` | `quay.io/ceph/demo`, pinned by digest | Ceph 19.2.0 (squid) with one monitor, manager, OSD, and gateway in one container |
| `s3-interop-<leg>-<store>` | `logit:s3-interop`, built from the current tree | one per `logit-<leg>.yaml` and store |
| (one-shot) | `amazon/aws-cli:2.39.2`, pinned by digest | lists, fetches, and heads every object, so the verdict doesn't rest on `logit`'s own signer |
| (one-shot) | `logit-s3-interop-tools:local`, from `Dockerfile.tools` | `check.py`, plus the stock `gzip` and `zstd` binaries |

`minio/minio` is no longer published on Docker Hub or Quay. `pgsty/minio` is the maintained
community build of the same server.

The legs, each run against both stores:

| Leg | Config | What it checks |
|---|---|---|
| `json-gzip` | `logit-json-gzip.yaml` | 1,000 events from a `generate_in` source with `count: 1000` land as gzipped JSON lines under `<prefix>dt=YYYY-MM-DD/hour=HH/<time>-<store id>-<seq>.ndjson.gz`; the key matches that form, `gzip -t` passes, `Content-Type` is `application/gzip` with no `Content-Encoding`, the lines hold every `seq` once, and no `*.dropped` counter is nonzero |
| `zstd`, `none` | `logit-zstd.yaml`, `logit-none.yaml` | the same for `.ndjson.zst` (`zstd -t`, `application/zstd`) and `.ndjson` (`application/x-ndjson`) |
| `native` | `logit-native.yaml`, `logit-replay-native.yaml` | `format: native` objects, gunzipped, replay through `replay_in` into `stdio_out` `format: json`, and the replay holds 1,000 events, each `seq` once |
| `vhost` | `logit-vhost.yaml` | 500 events with `path_style: false`. Only `<bucket>.<endpoint host>` resolves on the network, so a sink that sent a path-style request couldn't connect |
| `sse` | `logit-sse.yaml` | `sse: {algorithm: aes256}`: the stored object's `ServerSideEncryption` reads `AES256`. When the store refuses the header, the row is `GAP` with the store's answer, and `FAIL` unless the AWS CLI's own `PutObject` with the same header fails too |
| `creds` | `logit-creds.yaml` | a wrong secret key is `Refused`: the sink holds, `logit.component.retrying` reads 1, requests are all `4xx`, and no `*.dropped` is nonzero. The script stops it, restarts it with the right key, and the objects hold the spool's first-run events and the second run's, each `seq` once per run |
| `bucket` | `logit-bucket.yaml` | a bucket that doesn't exist is `Refused` and held the same way; the script creates the bucket while the sink retries, and the queue drains into it with every `seq` once |
| process exits | | every leg exits 0 on `SIGTERM` after its shutdown grace |

`creds` and `bucket` snapshot their telemetry 20 seconds in, before the fault clears, so the held
state is read from the run itself.

### Not covered

The harness runs on the single-`PutObject` path, so it has no multipart legs, no forced retry that
overwrites a key, no `3xx` from a wrong region, and no check of a store's answer to a signed
`x-amz-content-sha256` body. A `tc netem` reset during uploads isn't in it either. The plan's W5
section lists them; each needs either the multipart path or a fault injector.

## AWS mode

`S3_INTEROP_TARGET=aws script/s3-interop` runs the same legs against a real bucket. It starts no
store, and it reads the bucket and credentials from a git-ignored env file,
`perf/results/s3-interop.env` by default, or the file `S3_INTEROP_ENV` names:

```sh
S3_INTEROP_BUCKET=my-test-bucket
S3_INTEROP_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
# Optional: a bucket name nobody owns, which the `bucket` leg creates and deletes. Defaults to
# the bucket's name with a `-late-<timestamp>` suffix.
S3_INTEROP_LATE_BUCKET=
```

The script refuses to start when the file is missing, isn't git-ignored, or lacks the first four
keys, and it never prints a credential. Every leg writes under `s3-interop/<timestamp>-<pid>/`, so
runs against one bucket never read each other's objects, and the script deletes that prefix and the
late bucket when it ends (`S3_INTEROP_KEEP=1` leaves them). The credentials need `s3:PutObject`,
`s3:GetObject`, and `s3:ListBucket` on the bucket, `s3:CreateBucket` and `s3:DeleteBucket` for the
late bucket, and nothing else; the `bucket` leg reads `GAP` when the script can't create the late
bucket. Static keys only: a session token isn't read.

AWS addresses the buckets virtual-hosted in this mode, because path-style requests are deprecated
there. Nobody has run this mode yet.

## Running it

```sh
script/s3-interop
```

| Variable | Effect |
|---|---|
| `S3_INTEROP_SKIP_IMAGE=1` | reuse `logit:s3-interop` and `logit-s3-interop-tools:local` |
| `S3_INTEROP_OUT=<dir>` | where run directories go (default `perf/results/s3-interop`) |
| `S3_INTEROP_TARGET=aws` | [AWS mode](#aws-mode) |
| `S3_INTEROP_ENV=<file>` | AWS mode's env file |
| `S3_INTEROP_KEEP=1` | leave the stack up, and AWS mode's objects in place |

A run writes `perf/results/s3-interop/<timestamp>/` (git-ignored):

| File | Contents |
|---|---|
| `results.md` | the rows below |
| `fetch/<store>/` | every object, the listings, and each leg's `head-object` |
| `<leg>-<store>-telemetry.log` | each leg's `internal` telemetry and self-logs; the `creds` and `bucket` legs also keep a `-phase1` copy |
| `replay/<store>/` | the decompressed `native` objects and the events `replay_in` read back |
| `provenance.txt`, `exits.txt`, `timeline.log`, `logs/` | image digests and versions, each leg's exit code, the run's phases, and every container's log |

The `replay_in` leg decompresses first, because this tree's `replay_in` reads uncompressed files
only. Once `replay_in` reads `.gz` and `.zst` (the W4 branch), the decompression step in the script
can go.

The project name is fixed, so one run at a time per daemon: the script refuses to start while the
project has containers, and prints the `down` command for a stack a crashed run left behind.

## Recorded run

2026-10-30, local mode, `s3_out` at `20ed4753` (the `s3/w3a` branch) with this harness, against
MinIO `RELEASE.2026-08-04T00-00-00Z` and Ceph 19.2.0 (squid), 17 of 18 rows `PASS`, one `GAP`, no
`FAIL`, and no `s3_out` bug. The run directory is
`perf/results/s3-interop/20261030T184002Z/`.

| Leg | MinIO | Ceph RGW |
|---|---|---|
| json + gzip (default) | `PASS`: 1 object, 1000 events, each `seq` once | `PASS`, the same |
| json + zstd | `PASS` | `PASS` |
| json, no compression | `PASS` | `PASS` |
| native format, replayed | `PASS`: 1000 events back through `replay_in` | `PASS`, the same |
| virtual-hosted addressing | `PASS`: 500 events | `PASS`, the same |
| SSE-S3 header | `PASS`: `ServerSideEncryption` reads `AES256` | `GAP`: `400 InvalidRequest`, empty message, 74 `4xx` attempts held; the AWS CLI's `PutObject` with the header fails too |
| refused credentials | `PASS`: 14 `4xx`, `retrying` 1, no drop; 400 events after the restart | `PASS`: 29 `4xx`, the same |
| missing bucket | `PASS`: 13 `4xx`, `retrying` 1, no drop; 200 events once created | `PASS`: 29 `4xx`, the same |
| process exits | `PASS` | `PASS` |

What the run showed:

- **The Ceph demo container refuses SSE-S3.** It answers `400 InvalidRequest` with an empty body to
  `x-amz-server-side-encryption: AES256`; it has no key manager configured, and the harness doesn't
  configure one. `s3_out` reads the answer as `Refused`, as its response table says
  for `InvalidRequest`: it holds the queue and retries, and counts no `*.dropped`. The row is `GAP`
  because the store, not the sink, declines the header.
- **The refused legs are not vacuous.** Their phase-1 telemetry holds `4xx` requests and no `2xx`,
  and the objects appear only after the credentials or the bucket are fixed.
- **The `creds` restart exercises the spool.** The second process generates its own 200 events
  (`run: run2`); the first run's 200 (`run: run1`) come out of `buffer.disk:`, so each run's
  `seq` set is checked separately.
- **Negative controls.** Setting the `vhost` leg to `path_style: true` fails its row on both stores
  (26 and 32 `connect_error` attempts: the sink dials a name that doesn't resolve), and setting the
  `zstd` leg to `compression: none` fails its row (the key ends `.ndjson`, not `.ndjson.zst`).
  Before `rgw` lost its `hostname:`, the Ceph `vhost` row passed under `path_style: true`, because
  Docker's DNS resolved the bare name; the control caught that.
- **Ceph needed its address.** RGW treats any `Host` that is neither its DNS name nor an address as
  a bucket name, so the path-style legs dial the container's address.

## Cleanup and a shared daemon

Everything the script creates belongs to the `s3-interop` project, the `s3-interop-<leg>-<store>`
containers, and the one-shot AWS CLI and tools containers, which remove themselves. In AWS mode
it also creates the `s3-interop-net` network. The script captures every container's log into
`logs/` and removes them on every exit.
