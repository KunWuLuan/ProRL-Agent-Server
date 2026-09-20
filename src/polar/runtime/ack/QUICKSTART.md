# Quick Start: SWE Evaluation on ACK/ACS Sandboxes

The shortest path to a graded SWE rollout when the sandbox runs on the ACK/ACS
sandbox stack instead of local Docker. **Assumes the task images already exist**
in a registry the cluster can pull — nothing here builds an image.

This walkthrough drives that stack through Polar's `e2b` backend, because a
self-hosted E2B plane — `sandbox-manager` (control), `sandbox-gateway` (data) and
`agents.kruise.io` `SandboxSet` pools — *is* what ACK/ACS ships. Both remote
backends and all their docs live in this one package. To drive the same cluster
through Polar's `ack` backend instead (Pods and OpenKruise SandboxClaims straight
against the API server, no E2B plane), see
[Running the shipped examples](README.md#running-the-shipped-examples).

Cluster specifics are placeholders: `<registry>`, `<project>`, `<task-image>`,
`<tag>`, `<namespace>`, `<sandbox-namespace>`, `<kube-context>`,
`<sandbox-node>`, `<manager-port>`, `<gateway-port>`, `<admin-key>`,
`<served-model>`, `<pool-name>`.

Depth, allocation modes, RL training and troubleshooting:
[RUNBOOK.md](RUNBOOK.md). Backend reference for the native `ack` backend:
[README.md](README.md).

A ready-made deployment of §3 — control pod, Services, warm pool, bootstrap
ConfigMap and a no-model probe that verifies the network path before any model is
involved: [DEMO.md](DEMO.md).

## What you get

Two dataset formats drive the same runtime; only the evaluator differs.

| Path | Dataset shape | Conversion | Evaluator | Submitter |
| --- | --- | --- | --- | --- |
| **A — HF rows** | one JSON row per instance | `datasets` → local JSON cache | `swebench_harness` | `examples/swebench_verified/submit_swebench_tasks.py` |
| **B — Harbor task dirs** | a directory per task (`task.toml` + `tests/` + `environment/`) | `harbor download --export` | `harbor` | `examples/tmax-15k/submit_tmax_tasks.py` |

Both paths were verified end to end against a self-hosted E2B plane: path A
resolved `pylint-dev__pylint-4661` (reward `1.0`; ~300 s wall, of which
`init_ms` ≈ 198 s is the harness CLI installing into a cold sandbox, `run_ms`
≈ 6 s against a stub inference server, and `postrun_ms` ≈ 81 s is grading) and
path B resolved `adaptive-rejection-sampler` from a Terminal-Bench export
(reward `1.0`, ~60 s per session).

```
  submit ──▶ rollout server ──▶ gateway node ──┬──▶ sandbox pool (E2B / ACK)
   (you)         (control)         (control)   │        │  agent harness
                                               │        ▼
                                               └── LLM proxy ◀── agent calls
                                                         │
                                                         ▼
                                                   inference server
```

## 0. Prerequisites

```bash
uv sync --extra e2b --extra swebench
uv run python -c "from importlib.metadata import version; print('e2b', version('e2b'), 'swebench', version('swebench'))"
```

`uv sync` makes the environment match exactly the extras you list, so add
`--extra ack` when you also drive the native `ack` backend: omitting it
uninstalls `kubernetes`, after which `uv run pytest tests/runtime` reports
`1 skipped` rather than `4 passed`.

| Need | Notes |
| --- | --- |
| `swebench` **>=4,<5** | path A only. 5.x removed `swebench.harness.test_spec`, which `swebench_harness` imports. |
| `e2b` extra (`>=2.25,<2.51`) | paths A and B on the `e2b` backend. Per-request sandbox-id headers — verified on 2.50 — are what let one gateway URL serve every sandbox. **2.51 breaks self-hosted planes**: it creates sandboxes via `POST /v2/sandboxes`, which a v1-only manager answers with `405 Method Not Allowed`. `uv.lock` pins 2.50.0; a bare `pip install 'polar[e2b]'` does not. |
| Cluster access | `kubectl --context <kube-context>` with rights to create `SandboxSet`s in `<namespace>`. |
| A pullable image | Remote backends never build. `<registry>/<project>/<task-image>:<tag>` must already exist. |
| Inference endpoint | SGLang or vLLM (OpenAI-compatible), or the stub in [RUNBOOK → appendix](RUNBOOK.md#appendix-smoke-testing-without-a-model-server). |

## 1. Install and configure the sandbox components

### 1.1 Check the plane

A self-hosted E2B deployment is two pieces: **sandbox-manager** (control plane —
the E2B API for templates and sandboxes) and **sandbox-gateway** (data plane —
the envd proxy that reaches into each sandbox pod).

```bash
kubectl --context <kube-context> -n <sandbox-namespace> get deploy,svc,pods

kubectl --context <kube-context> -n <sandbox-namespace> \
  port-forward deploy/sandbox-manager <manager-port>:8080 &
curl -s localhost:<manager-port>/health
curl -s localhost:<manager-port>/v2/sandboxes | head -c 200     # expect [] or a list
```

`GET /v2/sandboxes` succeeding does **not** mean the v2 API is complete: the same
plane can still create sandboxes only on v1 `POST /sandboxes`, which is exactly
what the `e2b<2.51` cap in §0 is for.

The API key is the manager's admin key — read it, never commit it:

```bash
kubectl -n <sandbox-namespace> get deploy sandbox-manager \
  -o jsonpath='{.spec.template.spec.containers[0].args}' | tr ',' '\n' \
  | grep -o 'e2b-admin-key=[^"]*' | cut -d= -f2
```

### 1.2 Create one sandbox pool per image

The manager implements neither template build nor alias lookup, so templates are
created out of band: a pool object (`agents.kruise.io` `SandboxSet`) **is** the
template, and its name is what you pass as `runtime.kwargs.template`.

```yaml
apiVersion: agents.kruise.io/v1alpha1
kind: SandboxSet
metadata:
  name: <pool-name>                      # == runtime.kwargs.template
  namespace: <namespace>
spec:
  replicas: 2                            # >= 2 for path A (agent + grading sandbox)
  runtimes:
    - name: agent-runtime                # injects the envd sidecar
  template:
    spec:
      automountServiceAccountToken: false
      restartPolicy: Always
      nodeSelector:
        kubernetes.io/os: linux
        # kubernetes.io/hostname: <sandbox-node>   # only when the registry is
        #                                          # unreachable and the image is
        #                                          # already in that node's cache
      containers:
        - name: main
          image: <registry>/<project>/<task-image>:<tag>
          imagePullPolicy: IfNotPresent
          command: ["sleep", "infinity"]
          resources:
            requests: {cpu: "2", memory: 4Gi, ephemeral-storage: 10Gi}
          securityContext:
            runAsUser: 0
            privileged: false
            capabilities:
              add: ["SYS_RESOURCE"]      # required — see below
```

```bash
kubectl apply -f pool.yaml
kubectl -n <namespace> get sandboxset <pool-name> -w       # AVAILABLE reaches replicas
kubectl -n <namespace> get pods -w | grep '^<pool-name>-'  # members come up 2/2 Running
```

Four details that fail in confusing ways:

- **Select pool members by name, not by label.** A member of a hand-applied pool
  carries only `agents.kruise.io/created-by=sandbox`: `sandbox-pool` and
  `sandbox-claimed` exist only when the SandboxSet's own template sets them, and
  a claim adds `agents.kruise.io/owner` plus `sandbox-name` without ever flipping
  `sandbox-claimed` — which is exactly why the `E2B_SANDBOX_URL` warning below
  holds.
- **`CAP_SYS_RESOURCE` + root.** envd prefixes every command with an
  `oom_score_adj` write. Without the capability the prelude fails, so *the user
  command never runs*: every `commands.run` returns exit 1 with stderr
  `--: 1: echo: echo: I/O error`, while the files API keeps working.
- **`imagePullPolicy: IfNotPresent`.** With registry DNS unreachable this is the
  only way to use a node-cached image; pair it with the hostname `nodeSelector`.
  `kubectl get nodes -o json | jq '.items[].status.images'` reports what nodes
  *claim* to have — a pod that actually reaches `Running` is the only proof.
- **Pool size.** `refresh_runtime: true` (path A) uses **two** sandboxes per
  session concurrently; `refresh_runtime: false` (path B) uses one. An undersized
  pool degrades to on-demand creation, i.e. slow starts rather than errors.

### 1.3 Point the E2B SDK at the plane

`E2BRuntime` issues no HTTP of its own — every call is the official SDK
(`AsyncSandbox.create`, `sandbox.commands.run`, `sandbox.files.write/read`,
`sandbox.kill`, `AsyncTemplate.alias_exists/build`). Endpoint discovery therefore
belongs to the SDK's environment, exported in the **gateway** process (that is
where the runtime object is built):

| Variable | Value here | Notes |
| --- | --- | --- |
| `E2B_API_KEY` | `<admin-key>` | Required — `E2BRuntime.__init__` raises without it. |
| `E2B_API_URL` | `http://sandbox-manager.<sandbox-namespace>.svc.cluster.local:<manager-port>` | Control plane. |
| `E2B_SANDBOX_URL` | `http://sandbox-gateway.<sandbox-namespace>.svc.cluster.local:<gateway-port>` | Data plane — **one URL for every sandbox**. |
| `E2B_DOMAIN` | `<sandbox-domain>` | Only needed when the plane derives host names from a domain. |

`E2B_SANDBOX_URL` is the part that goes wrong most often:

- **Use the sandbox gateway.** The SDK sets `E2b-Sandbox-Id` and
  `E2b-Sandbox-Port` on every envd request (`e2b/sandbox_async/main.py`,
  verified on 2.50), and the gateway routes on them, so a single URL addresses
  all sandboxes concurrently.
- **Do not** point it at a Service that selects claimed sandbox pods. Claiming
  does not flip the pod labels, so such a Service has no endpoints and the first
  `commands.run` dies with `connection refused` — which reads like a sandbox
  startup failure.
- A per-sandbox envd port-forward (`E2B_DEBUG=true`, or
  `http://127.0.0.1:49983`) only ever addresses one sandbox: smoke tests only.

Smoke-test the plane through the SDK before involving Polar — two sandboxes at
once proves routing and concurrency:

```python
import asyncio
from e2b import AsyncSandbox

async def main():
    pool = "<pool-name>"
    sandboxes = [await AsyncSandbox.create(template=pool, timeout=600) for _ in range(2)]
    for sbx in sandboxes:
        result = await sbx.commands.run("hostname", user="root", timeout=30)
        print(sbx.sandbox_id, "->", result.stdout.strip())   # hostname must match its own id
    for sbx in sandboxes:
        await sbx.kill()

asyncio.run(main())
```

## 2. Convert the dataset

### Path A — HuggingFace rows

`examples/swebench_verified/dataset.py` downloads `princeton-nlp/SWE-bench_Verified`
with `datasets`, normalizes `FAIL_TO_PASS` / `PASS_TO_PASS` from JSON strings to
lists, and caches the rows to `~/.cache/polar/swebench_verified.json`. The first
submit does this automatically; run it once up front to see the cache and the
image each instance needs:

```bash
cd examples/swebench_verified
uv run python - <<'PY'
from dataset import base_image_for_instance, load_swebench_verified

rows = load_swebench_verified()                 # HF -> normalized JSON cache
print(f"cached {len(rows)} instances")

instance = next(r for r in rows if r["instance_id"] == "<repo>__<issue>")
print("image:", base_image_for_instance(instance))   # matches the pool's image
print("F2P:", instance["FAIL_TO_PASS"], "P2P:", len(instance["PASS_TO_PASS"]))
PY
```

`base_image_for_instance()` asks `swebench` for the authoritative image key, so
the printed reference is exactly what the pool's `image:` must be. For a first
run prefer an instance with **one** `FAIL_TO_PASS` and **no** `PASS_TO_PASS` —
grading takes seconds instead of an hour (more selection rules in
[RUNBOOK → Step 5](RUNBOOK.md#step-5--pick-an-instance-and-image)).

### Path B — Harbor task directories

Harbor hub serves task *directories*, not images. Export them once:

```bash
uv pip install harbor
harbor download '<org>/<dataset>-Harbor@latest' --export --output-dir <dataset-dir>
ls <dataset-dir>/*/task.toml | head
```

Each task directory must hold `task.toml`, `instruction.md`, `tests/test.sh` (the
verifier, which writes `/logs/verifier/reward.txt`) and `environment/Dockerfile`.
`task.toml` supplies the budget and — when present — `[environment].docker_image`,
which is the reference the pool must match:

```bash
grep -h docker_image <dataset-dir>/<task>/task.toml
```

The verifier runs **outside** the image, so the exported `tests/` directory has
to be readable by the *gateway* process. Run the submitter where the gateway runs,
or copy the task directory there first:

```bash
kubectl -n <namespace> cp <dataset-dir>/<task> <control-pod>:/data/tasks/<task> -c main
```

## 3. Start the control plane where sandboxes can reach it

The agent inside the sandbox calls the gateway's LLM proxy, so the gateway's
`public_url` must resolve **from inside a sandbox pod**. Run rollout + gateway in
the cluster, with one `ClusterIP` Service per process. Both select the same
control-plane pod: the selector picks the pod, the port picks the process.

Which URL is dialed by whom — and which of them may never be loopback — is
tabulated in
[network addressing](RUNBOOK.md#network-addressing-who-must-reach-whom).

```yaml
apiVersion: v1
kind: Service
metadata: {name: polar-rollout, namespace: <namespace>}
spec:
  selector: {app: polar-control}
  ports: [{name: rollout, port: 8080, targetPort: 8080}]   # submit, poll, results
---
apiVersion: v1
kind: Service
metadata: {name: polar-gateway, namespace: <namespace>}
spec:
  selector: {app: polar-control}
  ports: [{name: gateway, port: 8100, targetPort: 8100}]   # node API + LLM proxy
```

`topology.e2b.yaml`:

```yaml
rollout:
  host: 0.0.0.0
  port: 8080
  public_url: http://polar-rollout.<namespace>.svc.cluster.local:8080
  save_dir: /data/rollout_results
gateway:
  heartbeat_interval_seconds: 15
  nodes:
    - id: e2b-node-01
      host: 0.0.0.0
      port: 8100
      public_url: http://polar-gateway.<namespace>.svc.cluster.local:8100
      max_init_workers: 4
      max_run_workers: 4
      max_postrun_workers: 4
      model_served: <served-model>
      inference: {engine: sglang, base_url: "http://<inference-host>:8000"}
      default_runtime:            # fallback only — every task payload below
        backend: e2b              # carries its own runtime block
        image: <registry>/<project>/<task-image>:<tag>
        workdir: /polar/session/workspace
        kwargs: {template: <pool-name>, user: root}
```

```bash
set -a; . ./e2b.env; set +a       # E2B_API_KEY / E2B_API_URL / E2B_SANDBOX_URL
polar serve_rollout -c topology.e2b.yaml &
polar serve_gateway -c topology.e2b.yaml --node-id e2b-node-01 &
```

Inside a pod there is no `e2b.env` to source — put the same three variables in a
Secret and reference it with `envFrom`, so the admin key never lands on a disk:

```bash
kubectl -n <namespace> create secret generic polar-e2b \
  --from-literal=E2B_API_KEY=<admin-key> \
  --from-literal=E2B_API_URL=http://sandbox-manager.<sandbox-namespace>.svc.cluster.local:<manager-port> \
  --from-literal=E2B_SANDBOX_URL=http://sandbox-gateway.<sandbox-namespace>.svc.cluster.local:<gateway-port>
# pod spec:  envFrom: [{secretRef: {name: polar-e2b}}]
```

Confirm the node registered (from the control-plane host/pod), and confirm the
gateway is reachable **from a sandbox-namespace pod, not from a laptop**:

```bash
# 127.0.0.1 here is a health check run *on* the control-plane host, not a config value
curl -s http://127.0.0.1:8080/nodes | head -c 300      # expect your node id
kubectl -n <namespace> run net-check --rm -i --restart=Never \
  --image=<registry>/<project>/<tiny-image>:<tag> -- \
  sh -c 'wget -qO- http://polar-gateway.<namespace>.svc.cluster.local:8100/health'
```

A slim control image (`python:3.12-slim`) ships neither `curl` nor `wget`; the
same two checks through Python:

```bash
python -c 'import urllib.request as u; print(u.urlopen("http://127.0.0.1:8080/nodes", timeout=10).read()[:300])'
kubectl -n <namespace> run net-check --rm -i --restart=Never --image=python:3.12-slim -- \
  python -c 'import urllib.request as u; print(u.urlopen("http://polar-gateway.<namespace>.svc.cluster.local:8100/health", timeout=10).read())'
```

To submit from a laptop instead, port-forward the Service and hand the *submitter*
a copy of the topology whose `rollout.public_url` is the forwarded address:

```bash
kubectl -n <namespace> port-forward svc/polar-rollout 18081:8080 &
# topology.local.yaml — read by the submitter only:
#   rollout.public_url: http://127.0.0.1:18081   # test-only: names the tunnel, dies with it
```

The submitter reads nothing but `rollout.public_url` from that file
(`examples/swebench_verified/submit_swebench_tasks.py:284`), so the copy cannot
disturb the running services. Keep the services on `topology.e2b.yaml`: their
`gateway.nodes[].public_url` still has to resolve from inside a sandbox pod, and
loopback there points at the sandbox itself.

## 4. Required variables

Environment (gateway process):

| Variable | Required | Why |
| --- | --- | --- |
| `E2B_API_KEY` | yes | `E2BRuntime.__init__` raises without it. |
| `E2B_API_URL` | self-hosted plane | Control plane override; defaults to `https://api.<domain>`. |
| `E2B_SANDBOX_URL` | self-hosted plane | Data plane override; must be the sandbox **gateway**. |

Topology (`topology.e2b.yaml`):

| Field | Required | Notes |
| --- | --- | --- |
| `gateway.nodes[].public_url` | yes | Injected into the sandbox as `OPENAI_BASE_URL` / `ANTHROPIC_BASE_URL`; must resolve from a sandbox pod. Also dialed by the rollout server for session dispatch. `127.0.0.1`/`localhost` is **test-only** — a single-host smoke test — and never valid for a sandbox in a cluster. |
| `gateway.nodes[].model_served` | yes | The model id the gateway rewrites requests to. |
| `gateway.nodes[].inference.{engine,base_url}` | yes | `sglang` or `vllm`. `base_url` is dialed by the gateway process, so loopback is fine *only* when the engine is co-located with it. |
| `rollout.public_url` | yes | Where submitters POST `/rollout/task/submit`, and the default for `gateway.rollout_server_url`. Must resolve from every submitter, dashboard and gateway host; `127.0.0.1` is **test-only** (co-located processes, or a laptop `port-forward` copy used by the submitter alone). |
| `gateway.rollout_server_url` | no | Defaults to `rollout.public_url`. Set it only when the gateway reaches the rollout server by another address. |
| `gateway.nodes[].max_{init,run,postrun}_workers` | no | Concurrency per stage; `1` serializes everything. |

Per task (payload — the submitter builds all of these):

| Field | Required | Notes |
| --- | --- | --- |
| `runtime.backend` | yes | `e2b` here. Overrides the node's `default_runtime` **wholesale**. |
| `runtime.image` | yes | Registry reference; used for the derived template alias and for logs. |
| `runtime.kwargs.template` | yes (self-hosted) | Pool name. Without it `start()` tries to build a template the plane cannot build. |
| `runtime.kwargs.user` | no | Defaults to `root`; keep it — non-root drops capabilities. |
| `runtime.workdir` | yes | Agent CWD. `prepare` must create it. |
| `runtime.prepare` | yes | INIT recipe; installs the harness CLI. |
| `agent.harness` + `agent.model_name` | yes | e.g. `mini_swe_agent`; the model id is rewritten to `model_served`. |
| `evaluator.strategy` + `config` | yes | `swebench_harness` (path A) or `harbor` (path B). |
| `evaluator.refresh_runtime` | yes | `true` for path A, `false` for path B — see below. |
| `timeout_seconds` | yes | Whole-session budget shared by INIT + RUN + POST_RUN. |

CLI flags that must be set for a remote run:

| Flag | Path | Value |
| --- | --- | --- |
| `--runtime-backend` | A, B | `e2b` |
| `--runtime-kwargs template=<pool-name>` | A, B | Repeatable; values parse as JSON. The native `ack` backend takes `namespace=<ns>` instead — [Running the shipped examples](README.md#running-the-shipped-examples). |
| `--image-template` | A | Required — e.g. `docker.io/{image_key}`; placeholders `{instance_id}`, `{slug}`, `{image_key}`. `{image_key}` is the `swebench`-derived name and already carries a tag. Remote backends pull from a registry, so a local docker tag is not addressable. |
| `--image-template` | B | Optional — only when `task.toml` has no `[environment].docker_image`; placeholders `{task}`, `{slug}`. |
| `--refresh-runtime` / `--no-refresh-runtime` | A | Leave the default (`true`). |
| `--topology` | A, B | The topology whose `rollout.public_url` you can reach. |
| `--dataset-dir` / `--task` | B | Exported Harbor dir and task name(s). |

## 5. Run

### Path A — HF rows (SWE-bench Verified)

```bash
cd examples/swebench_verified
uv run python submit_swebench_tasks.py \
  --harness mini_swe_agent \
  --instance-id <repo>__<issue> \
  --runtime-backend e2b \
  --runtime-kwargs template=<pool-name> \
  --image-template "docker.io/{image_key}" \
  --topology /path/to/topology.e2b.yaml \
  --timeout-seconds 2400
```

Wider runs: drop `--instance-id` and use `--max-tasks 10`, or add
`--num-samples 8` for pass@8. The same run through the native `ack` backend —
including when to use Pod mode instead of a warm pool — is in
[Running the shipped examples](README.md#running-the-shipped-examples).

Expected output:

```
  [<repo>__<issue>]  resolved=1/1  (1/1 done)
  Tasks resolved (>=1):  1/1  (100.0%)
```

**Why `refresh_runtime` must stay `true` here.** The agent works in a *copy*
(`/polar/session/workspace`, staged by `prepare`), while grading applies the
extracted patch to the pristine `repo_dir` (`/testbed`) and runs the harness's
`eval.sh`. The patch is only applied when a **fresh** eval runtime exists, so
`--no-refresh-runtime` grades an untouched `/testbed`: the session still reports
`COMPLETED` with `patch_successfully_applied: true` and `exit_code: 0`, but the
`FAIL_TO_PASS` test fails (typically a missing dependency the patch adds to
`install_requires`). Reward `0.0` with no error is the signature.

### Path B — Harbor task directory

```bash
cd examples/tmax-15k
uv run python submit_tmax_tasks.py \
  --dataset-dir <dataset-dir> \
  --task <task> \
  --harness mini_swe_agent \
  --runtime-backend e2b \
  --runtime-kwargs template=<pool-name> \
  --workdir /app \
  --topology /path/to/topology.e2b.yaml
```

`--dataset-dir` must be the path **as the gateway sees it** (see §2). The session
budget defaults to `task.toml`'s `agent.timeout_sec + verifier.timeout_sec + 120`;
override with `--timeout-seconds`.

Expected output:

```
  [<task>]  resolved=1/1  (1/1 done)
  Tasks resolved (>=1):  1/1  (100.0%)
```

**Why `refresh_runtime` is `false` here.** The `harbor` evaluator reproduces
Harbor's contract: inject `tests/` into the container the agent just used, run
`bash /tests/test.sh`, read `/logs/verifier/reward.txt`. A Harbor verifier
inspects the **final state** of that container, so grading in a fresh sandbox
would see an empty one.

Two image realities for stock Harbor task images:

- They are often bare (an unmodified `ubuntu:24.04`), i.e. **no `curl`, no Node,
  sometimes no compiler**. `submit_tmax_tasks.py` bootstraps `curl` from apt
  before the uv-based harness install (`UV_CURL_BOOTSTRAP`); the Node CLIs
  (`codex`, `claude_code`, `opencode`, `qwen_code`, `pi`) need an image that
  already has `npm`, which is what `build_images.py` adds for local runs.
- The verifier's `test.sh` usually needs egress at grading time (`apt-get
  update`, `astral.sh/uv`, PyPI wheels). Check from inside a pool sandbox before
  a long run:

```bash
kubectl -n <namespace> exec <pool-pod> -c main -- bash -lc \
  'apt-get update -qq >/dev/null && echo apt-ok;
   curl -sS -o /dev/null -w "astral:%{http_code}\n" https://astral.sh/uv/install.sh;
   curl -sS -o /dev/null -w "pypi:%{http_code}\n" https://pypi.org/simple/'
```

## 6. Verify the run is genuinely green

Read the persisted session record under `rollout.save_dir`:

```bash
uv run python - <<'PY'
import json, pathlib
path = sorted(pathlib.Path("/data/rollout_results").glob("*/ses_*.json"))[-1]
data = json.loads(path.read_text())
evaluation = (data["trajectory"]["metadata"] or {}).get("evaluation", {})
print("status:", data["status"], "error:", data["error"])
print("timing(s):", {k: round(v / 1000, 1) for k, v in data["timing"].items()})
print("reward:", evaluation.get("outcome_reward"))
report = evaluation.get("report") or {}
grading = report.get("grading_report") or {}
f2p = (grading.get("tests_status") or {}).get("FAIL_TO_PASS") or {}
print("resolved:", report.get("resolved"),
      "| applied:", grading.get("patch_successfully_applied"),
      "| F2P success:", f2p.get("success"))
PY
```

All of these must hold:

- `status == "COMPLETED"` and `error is None`.
- Path A: `report.resolved == true` and `report.failed_apply_patch == false`,
  with `report.grading_report.tests_status.FAIL_TO_PASS.success` non-empty and
  `grading_report.patch_successfully_applied == true`. Path B:
  `evaluation.resolved == true` with `verifier_exit_code == 0`.
- `trajectory.traces` is non-empty and each trace carries `prompt_ids`,
  `response_ids` and `response_logprobs` — proof the LLM proxy captured the calls.
- Completions persisted under `save_dir`.

## 7. Teardown

```bash
kubectl -n <namespace> delete sandboxset <pool-name>
kubectl -n <namespace> delete svc polar-rollout polar-gateway
kubectl -n <namespace> delete pod net-check --ignore-not-found
curl -s -H "X-API-KEY: <admin-key>" \
  http://sandbox-manager.<sandbox-namespace>.svc.cluster.local:<manager-port>/v2/sandboxes
```

The last call must return an empty list: `stop()` → `sandbox.kill()` deletes the
sandbox pod, and the pool object refills its warm replicas, so nothing
accumulates per session. A leaked sandbox means a session was killed mid-run.
