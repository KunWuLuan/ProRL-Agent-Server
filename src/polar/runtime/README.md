# Runtime Backends

`polar.runtime` gives each rollout session its own **sandbox** — one container
(a local Docker or Apptainer container, an E2B cloud sandbox, or a Kubernetes
pod) that lives for the whole session. The gateway uses it to run the prepare
recipe, execute the agent and evaluator commands, move files in and out, then
tear it down.

## Mental model

- **One `RuntimeSpec` → one container**, shared across the init → run → eval
  stages of a session.
- On the local backends the host session directory is **bind-mounted** to a
  fixed in-container path, `/polar/session` (`RUNTIME_SESSION_DIR`).
  Uploads/downloads under that path are plain host-side file copies (fast);
  paths outside it fall back to `docker cp` / `tar` streaming. The remote
  backends (E2B, ACK) have no bind mount and always transfer over the wire.
- Commands run in a login shell (`bash -lc`) with working directory
  `cwd or spec.workdir or /polar/session`.
- The factory verifies the chosen backend actually supports what the spec asks
  for (GPUs, CPU/memory limits, internet-off) before building it.

## Main files

- `models.py`: `RuntimeSpec`, `PrepareAction`, `ExecInput`, `ExecResult`.
- `base.py`: the `BaseRuntime` contract, the `/polar/session` path constants, and
  the bind-mount copy helpers.
- `docker.py`: `DockerRuntime` — the default backend.
- `apptainer.py`: `ApptainerRuntime` — daemonless, for clusters.
- `e2b.py`: `E2BRuntime` — E2B cloud sandboxes (optional `e2b` extra).
- `ack/`: `ACKRuntime` — Kubernetes pods and OpenKruise sandbox pools (optional
  `ack` extra). A package rather than a single module: `_sdk.py` holds the extra
  guard and cluster constants, `_util.py` the manifest helpers, `client.py` the
  shared client manager, `runtime.py` the backend itself. See
  [ack/README.md](ack/README.md).
- `factory.py`: backend lookup + capability validation; also loads a custom
  backend via `RuntimeSpec.import_path`. `e2b`/`ack` are imported lazily, so a
  base install never needs their SDKs.

## The contract

A backend implements `start`, `stop`, `exec`, `upload_file`, `upload_dir`,
`download_file`, `download_dir` (plus `cancel`), hiding container details from
harnesses and evaluators. Well-known in-container paths (from `base.py`) are
`/polar/session` and, under it, `artifacts/`, `logs/`, `logs/agent/`,
`logs/eval/`, and `eval_artifacts/`.

## Prepare recipe

`RuntimeSpec.prepare` and `RuntimeSpec.eval_prepare` are ordered lists of
`PrepareAction` steps:

- `upload_file`: copy one host file in.
- `upload_dir`: copy one host directory in.
- `exec`: run a command inside the container.

`prepare` runs before the agent. `eval_prepare` runs before evaluation — and if
it's omitted, the eval runtime simply replays `prepare`.

## Docker vs Apptainer

Docker is the default for local examples and supports `--cpus` / `--memory`
limits. Apptainer is daemonless (good for clusters that forbid the Docker
socket), uses a host-backed overlay, and exposes GPUs with `--nv`. Both
bind-mount the session directory and run commands via `bash -lc`, so harnesses
and evaluators behave the same on either.

## Remote backends (E2B, ACK)

For a full walkthrough — control-plane placement, RBAC, task payload, image
selection for multi-image datasets, verification checklist, troubleshooting, and
driving these backends from a Slime RL training loop — see
[RUNBOOK.md](RUNBOOK.md).

`E2BRuntime` and `ACKRuntime` run the session off-host, so `start()` creates the
well-known `/polar/session` directories inside the sandbox and
`resolve_host_path()` always returns `None`. With no bind mount, anything written
to the host `session_dir` is invisible inside the sandbox — code that stages a
file for an in-runtime command must go through `BaseRuntime.publish_file()`,
which is a no-op for bind-mounted backends and an upload for remote ones (the
patch evaluators use it for `patch.diff` and `eval.sh`). Install the SDK you need:

```bash
uv pip install 'polar[e2b]'   # E2B cloud sandboxes; also needs E2B_API_KEY
uv pip install 'polar[ack]'   # Kubernetes pods; also needs a kubeconfig
```

E2B builds (or reuses) a sandbox **template** from `spec.image` and starts one
sandbox per session. `cpus` / `memory_mb` are baked in at template build time,
`allow_internet: false` creates the sandbox offline, and `kwargs.allow_out` sets
an egress allowlist. Commands are dispatched in the background and waited on
locally, so a step timeout surfaces as Polar's `return_code == -1`.

```yaml
runtime:
  backend: "e2b"
  image: "registry.example.com/polar/swebench:latest"
  cpus: 4
  memory_mb: 8192
  kwargs:
    template: "polar-swebench"   # optional: reuse a pre-built alias
```

ACK creates one `sleep infinity` Pod per session — or, with
`kwargs.use_sandbox_claim`, claims a pre-warmed sandbox from an OpenKruise
`SandboxSet` pool, which starts sessions much faster at high rollout concurrency.
The pool is shared across sessions and deliberately left running on `stop()`.
Unlike Harbor's `ACKEnvironment`, this backend never builds images: `spec.image`
must already be pushed somewhere the cluster can pull it.

File transfers stream a tar over the Kubernetes exec websocket in binary mode
(`binary=True`), so binary artifacts survive the round trip; the default text
mode decodes each frame as UTF-8 and silently corrupts them.

RBAC, the full `kwargs` table, `pod_overrides` and volume handling, the
OpenKruise claim lifecycle, pool sizing, teardown semantics and ACK-only
troubleshooting are in [ack/README.md](ack/README.md).

Neither remote backend supports GPUs, and ACK cannot disable internet access, so
the factory rejects specs that ask for those.

`kwargs.volumes` is **not** rejected — it is read only by `docker` and
`apptainer`, and remote backends ignore it silently, since there is no host to
mount from. A config ported from a bind-mounted backend therefore loses its
mounted tooling without any error; bake it into the image, install it in
`prepare`, or on ACK express a real volume through `kwargs.pod_overrides`.

`spec.image` is resolved per session, and `TaskRequest.runtime` is per task, so
one deployment can run a dataset whose instances need different images. The
derivation rules, and what each mode costs when the image count is large, are in
the runbook's
[One dataset, many images](RUNBOOK.md#one-dataset-many-images).
