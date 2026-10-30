# CRI container-log interop fixtures

The pod log trees a real kubelet leaves under `/var/log/pods`, written by containerd and by CRI-O,
copied out of a Kubernetes node. `cri_in` (`crates/logit-inputs/src/cri.rs`) and its record parser
(`crates/logit-proto/src/cri.rs`) were written from the runtimes' source and the kubelet's reader;
these files check that reading against what the runtimes put on disk. The test is
`crates/logit-inputs/tests/cri_interop.rs`. It copies a tree to a scratch directory, runs `cri_in`
over it, and compares the lines with what the pods wrote (`tools/record-fixtures/cri/pods.yaml`),
not with `cri_in`'s own output.

To regenerate, run `script/record-fixtures cri` (or `cri-containerd` or `cri-crio` for one runtime).
It pulls a pinned `kind` and `minikube` binary (each checked against a SHA-256), their node
images, and BusyBox, and deletes both clusters when it ends. See `../README.md` for the
corpus-wide rules. A re-run changes the pod uids, container ids, times, and the number of rotated
files; the test reads those from the tree and doesn't pin them.

The repository's `.gitignore` excludes `*.log`, so commit a new recording with
`git add -f testdata/interop/cri`.

## Producers

| Directory | Runtime | Node | Recorded |
|---|---|---|---|
| `containerd/` | containerd v2.3.4 | `kind` v0.33.0, node image `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5` (Kubernetes v1.37.0), one control-plane node | 2026-10-30 |
| `crio/` | CRI-O 1.35.7 (conmon) | `minikube` v1.39.0 with `--driver=docker --container-runtime=cri-o` (kicbase v0.0.51, Kubernetes v1.37.0) | 2026-10-30 |

Each directory holds `pods/` (the tree), `symlinks.txt` (the kubelet's `/var/log/containers`
symlinks as `NAME -> TARGET`), and `versions.txt`. BusyBox 1.37.0 ran every container.

Both nodes run a real kubelet with `containerLogMaxSize: 24Ki` and
`containerLogMonitorInterval: 3s` (the kubelet rejects anything under 3 s), so the kubelet itself
renames a live file to `<n>.log.<YYYYMMDD-HHMMSS>`, asks the runtime to reopen the log, and
gzips the previous rename on the next rotation. For `crio/` the settings are appended to
`/var/lib/kubelet/config.yaml` and the kubelet restarted; `kind` takes them from
`tools/record-fixtures/cri/kind-config.yaml`.

## What each pod exercises

Pods live in the namespace `cri-fixture`, so each directory is `cri-fixture_<pod>_<uid>`.

| Pod and container | Files | Construct |
|---|---|---|
| `edge`, `writer` | `0.log.<ts>`, `0.log` | One run, rotated by the kubelet part way through. Lines over the runtime's split size: 20,500 bytes of `y` with no newline until after a `stderr` line (a `P` run on stdout with a whole stderr record inside it), 17,384 bytes of `x`, and 16,383 `a` bytes, an e-acute, and 100 `b` bytes (containerd's 16,384-byte cut falls between the two bytes of the e-acute; conmon's pieces don't). A CRLF line, a line with an inner `\r`, an empty line, invalid UTF-8 (`\xff\xfe`), `stdout` and `stderr` interleaved, and an unterminated `held across rotation` the kubelet's reopen splits: the head is a `P` at the end of `0.log.<ts>` and ` rest` is the first stdout record of the new `0.log`. The run ends on an unterminated line (`dangling no newline`), a final `P` |
| `edge`, `writer` | `1.log` and later (three in `containerd/`, two in `crio/`) | Container restarts. Each run is `second run` and an unterminated `second run dangling`. The kubelet keeps restarting the container until the recording ends, so the count varies. Their files carry restart counts 1 and up |
| `rotator`, `spin` | `0.log`, `0.log.<ts>`, two `0.log.<ts>.gz` | 850 numbered lines and an unterminated tail, rotated three times: two compressed (never read by `cri_in`), one newest rotated file left uncompressed, and the live file |
| `static-hello-<node>`, `hello` | `0.log` | A static pod from `/etc/kubernetes/manifests`. Its directory carries the kubelet's 32-character manifest hash where a regular pod has an API uid |

`symlinks.txt` holds one entry per container attempt, named
`<pod>_<namespace>_<container>-<64 hex container id>.log`, with an absolute target under
`/var/log/pods`. `edge` has one per attempt. The test checks each name against its target
and the tree.

## What the recordings settled

The recordings agree with `docs/adr/cri-container-logs.md` on the timestamp forms, split size,
rotated file names, and symlink form. Observations the ADR doesn't state:

- **Timestamps.** containerd writes `Z` and trims trailing zeros, so the fraction is 6 to 9
  digits wide (`.5260678Z`). conmon writes a fixed nine digits and `+00:00`, and every record
  from one `read(2)` of a stream carries the same time.
- **Split size.** containerd cut at 16,384 bytes. conmon's pieces are one read's bytes, so their
  sizes follow how the writer's pipe delivered them: `y` is 4,096 four times then 4,116; `x` is
  8,192, 8,192, 1,000; `a` is 8,192, 8,191, 2, and 100. A line whose newline arrives in a later
  read ends in an empty `F` record.
- **A late first piece.** In a trial recording, containerd held an unterminated 16,394-byte write
  on stdout until the kubelet's reopen flushed it as a 16,384-byte `P` and a 10-byte `P`. It
  writes a `P` only once more than 16,384 bytes are buffered, and it reads in 4,096-byte
  chunks. The committed 20,500-byte line is long enough to be cut at once.
- **CRLF.** containerd removed the `\r`; conmon kept it (`crlf line\r`).
- **Split across a rotation.** Both runtimes wrote the head of an unterminated run to the renamed
  file as a `P` and the continuation to the new file, so that run leaves `cri_in` as two lines
  (ADR section 4).
- **Container garbage collection.** The kubelet removes an older dead attempt's directory within
  a minute of the next attempt starting. A recording that copied the tree once at the end
  found only `2.log`, so the script copies `edge`'s directory every half second while it runs.
- **Lossy conversion.** `cri_in` replaces the invalid bytes with U+FFFD and diagnoses
  `invalid_utf8`; the test pins one occurrence per runtime.

## Preparation and trimming

- Only the `cri-fixture` namespace is copied. The cluster's own pods (`kube-system` and the rest)
  and their symlinks are left out.
- File contents are as the node held them, gzipped files included.
- `crio/versions.txt` is the script's output: the first lines of `crio --version`, the Kubernetes
  version, and the BusyBox image id. The `kubelet: not found` line is the lookup on the node's
  `PATH` that fails before the script tries `/var/lib/minikube/binaries/`.
- Both recordings ran BusyBox 1.37.0, index digest
  `sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec`; the CRI-O node's
  `versions.txt` shows the amd64 manifest inside it, `66a6306db78bf`. The recordings pulled the
  tag `busybox:1.37.0`; the pod specs now pin that digest, which is the one the tag resolved to
  when the containerd recording ran, and use the full `docker.io/library` name that CRI-O needs.
- Each directory is about 110 KB: 110,555 bytes for `containerd/` and 114,080 for `crio/`, mostly
  the 55 KB rotated `edge` file and the rotator's files. Rotation needs files over 24 KiB.

## Recording notes

- `kind load docker-image` fails against a Docker daemon that uses the containerd image store, so
  the nodes pull BusyBox themselves.
- A new namespace's `default` service account appears a moment after the namespace, so the
  script retries `kubectl apply`.
- `kind` and `minikube` run under `sudo` when `$DOCKER` is `sudo docker`, and `minikube` needs
  `--force` as root. The script deletes both clusters on exit, and removes the root-owned
  `minikube` home.
- The script pins linux-amd64 binaries and stops on other machines.

## Fuzz seeds

`fuzz/seedgen` reads the uncompressed files here and writes a few records of each as seeds for the
`cri` fuzz target: the first two short records, the first `P`, the first empty one, and the first
with invalid UTF-8. Regenerate them with `script/unsafe-check fuzz-seed` after a new recording.

## What isn't covered here (yet)

- Other runtimes and versions: one containerd (2.3.4) and one CRI-O (1.35.7), each on Kubernetes
  v1.37.0. cri-dockerd's Docker-format files and older conmon versions aren't recorded.
- `containerLogMaxFiles` pruning: the kubelet kept every rotated file, so no recording shows it
  deleting the oldest.
- A line over `max_line_bytes`: no pod writes one, so the oversize drop is covered only by unit
  tests.
- A TTY container.
- Non-UTC node time zones, which give containerd a `+hh:mm` offset instead of `Z`.
