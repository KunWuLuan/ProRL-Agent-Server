# Runbook: SWE Evaluation on a Remote Sandbox Runtime

How to run a SWE-bench-style evaluation end to end when the rollout sandbox lives
on a **remote** runtime backend — `ack` (Kubernetes / Alibaba Cloud ACK-ACS with
OpenKruise sandboxes) or `e2b` — instead of local Docker or Apptainer.

Everything below is backend-agnostic unless a step says otherwise. Cluster
specifics are written as placeholders: `<namespace>`, `<kube-context>`,
`<registry>`, `<control-host>`.

Related reading: [runtime backends](README.md),
[gateway](../gateway/README.md), [evaluators](../trajectory/evaluator/README.md),
[the SWE-bench example](../../../examples/swebench_verified/README.md).

## Why remote changes the topology

With Docker/Apptainer the gateway and the sandbox share a filesystem, so the
gateway can write a file into the session directory and the runtime sees it
through a bind mount. Remote backends have **no bind mount**, and the agent
inside the sandbox must reach the gateway over the network — the gateway injects
`OPENAI_BASE_URL` / `ANTHROPIC_BASE_URL` / `GOOGLE_API_URL` (with the API key set
to the session id) and transparently proxies every LLM call.

Two consequences drive the whole runbook:

1. **The gateway's `public_url` must be resolvable and reachable from inside the
   sandbox.** Running the control plane on a laptop and the sandbox in a cluster
   does not work without an inbound tunnel. The simplest reliable layout is to
   run the control plane *in the same cluster* and expose the gateway through a
   `ClusterIP` Service.
2. **Files staged for an in-runtime command must be pushed, not written.** Use
   `BaseRuntime.publish_file()`; see [the runtime contract](#runtime-contract).

```
  submit ──▶ rollout server ──▶ gateway node ──┬──▶ runtime (ACK sandbox / E2B)
   (you)         (control)         (control)   │        │  agent harness
                                               │        ▼
                                               └── LLM proxy ◀── agent calls
                                                         │
                                                         ▼
                                                   inference server
```

## Prerequisites

| Need | Notes |
| --- | --- |
| `polar[ack]` or `polar[e2b]` | `uv sync --extra ack --extra swebench` |
| `swebench` **>=4,<5** | 5.x removed `swebench.harness.test_spec`, which `swebench_harness` imports. `uv.lock` pins a working version; a bare `pip install polar[swebench]` may not. |
| Cluster access | A kubeconfig context with RBAC below, or in-cluster ServiceAccount. |
| A pullable image | Remote backends never build. `spec.image` must already exist in a registry the cluster can pull. |
| Inference endpoint | SGLang or vLLM (OpenAI-compatible). See [Appendix A](#appendix-a-smoke-testing-without-a-model-server) for a stub. |

## Step 1 — Cluster RBAC

The control plane creates and tears down pods and OpenKruise sandboxes. Grant the
minimum below in `<namespace>`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: polar-runtime, namespace: <namespace>}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["pods/exec", "pods/attach"]
    verbs: ["create", "get"]
  - apiGroups: [""]
    resources: ["pods/log", "events"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["agents.kruise.io"]          # SandboxClaim mode only
    resources: ["sandboxsets", "sandboxclaims", "sandboxes"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

Bind it to the ServiceAccount the control plane runs as. `kubectl auth can-i`
each verb before debugging anything else — a 403 on `sandboxclaims` surfaces as a
`RuntimeError` from `ACKRuntime.start()`.

## Step 2 — Choose the allocation mode

| Mode | `kwargs` | Start latency | Use when |
| --- | --- | --- | --- |
| ACK Pod | — | image pull + schedule | one-off runs, no OpenKruise in the cluster |
| ACK SandboxClaim | `use_sandbox_claim: true` | seconds (warm pool) | many concurrent rollouts |
| E2B | — | template build once, then seconds | no cluster at all |

SandboxClaim mode keeps a warm `SandboxSet` pool. Size it for **two sandboxes per
session** when `evaluator.refresh_runtime` is true (one for the agent, one fresh
one for grading). Claims set `createOnNoStock: true`, so an undersized pool
degrades to on-demand creation rather than failing.

## Step 3 — Runtime spec

`RuntimeSpec` is the only configuration surface. Fields that matter remotely:

```yaml
runtime:
  backend: "ack"                       # or "e2b"
  image: "<registry>/<project>/<task-image>:<tag>"
  workdir: "/polar/session/workspace"  # agent CWD; created by `prepare`
  cpus: 2
  memory_mb: 4096
  env:                                 # merged into every exec
    HOME: "/polar/session/home"
    PATH: "/opt/harness/bin:/usr/local/bin:/usr/bin:/bin"
  prepare:                             # INIT recipe, runs once per session
    - type: exec
      command: "..."
  eval_prepare:                        # recipe for the fresh grading runtime
    - type: exec
      command: "..."
  kwargs:
    namespace: "<namespace>"           # required for ack
    context: "<kube-context>"          # optional; omit for in-cluster config
    image_pull_secret: "<pull-secret>" # optional
    use_sandbox_claim: true
    sandboxset_name: "<pool-name>"
    sandboxset_replicas: 4
    claim_timeout: 900
    pod_ready_timeout: 900
```

E2B equivalents: `kwargs.template` reuses a pre-built sandbox template,
`allow_internet: false` creates the sandbox offline, and `kwargs.allow_out` sets
an egress allowlist. `E2B_API_KEY` must be present in the gateway's environment.

### Writing `prepare` / `eval_prepare` for a remote sandbox

These run through `runtime.exec()`, not on the host. Four rules that bite:

1. **Create `$HOME` if you point it inside `/polar/session`.** `start()` creates
   `artifacts/`, `logs/agent/`, `logs/eval/` and `eval_artifacts/` — nothing
   else. `git config --global` exits **255** when `$HOME` does not exist.
2. **Put the harness on `PATH` yourself.** Agent presets prepend
   `$HOME/.local/bin` at *run* time only; `prepare` runs with `spec.env` as-is.
3. **Make installs idempotent.** Warm-pool sandboxes are reused across attempts
   only until they are torn down; a guard turns a 10-minute install into a
   no-op:
   `[ -x /opt/harness/bin/<cli> ] || { python -m venv /opt/harness && /opt/harness/bin/pip install <pkg>; }`
4. **Use a regional package mirror when the cluster is far from the default
   index.** Harness installs dominate INIT otherwise.

`eval_prepare` should stay minimal — the grading runtime only needs git config
and a writable `$HOME`; the evaluator uploads `patch.diff` and `eval.sh` itself.

## Step 4 — Deploy the control plane where sandboxes can reach it

Run rollout + gateway (+ inference) in `<namespace>`, then expose the gateway:

```yaml
apiVersion: v1
kind: Service
metadata: {name: polar-gateway, namespace: <namespace>}
spec:
  selector: {app: polar-control}
  ports: [{port: 8100, targetPort: 8100}]
```

`topology.yaml` — the only non-obvious line is `public_url`, which is what gets
injected into the sandbox:

```yaml
rollout:
  host: 0.0.0.0
  port: 8080
  public_url: http://127.0.0.1:8080     # control-plane-local is fine
  save_dir: /data/rollout_results
gateway:
  rollout_server_url: http://127.0.0.1:8080
  nodes:
    - id: node-01
      host: 0.0.0.0
      port: 8100
      public_url: http://polar-gateway.<namespace>.svc.cluster.local:8100
      model_served: <served-model-name>
      inference: {engine: sglang, base_url: "http://127.0.0.1:8000"}
```

```bash
polar serve_rollout -c topology.yaml
polar serve_gateway -c topology.yaml --node-id node-01
```

Verify reachability **from a pod, not from your laptop**:

```bash
kubectl -n <namespace> run net-check --rm -it --restart=Never \
  --image=<registry>/<tiny-image> -- \
  sh -c 'wget -qO- http://polar-gateway.<namespace>.svc.cluster.local:8100/health'
```

Submit from your laptop with `kubectl port-forward svc/polar-rollout 8080:8080`,
or exec into the control pod.

## Step 5 — Pick an instance and image

Use the library, not string munging — `swebench` derives the authoritative image
key:

```python
from swebench.harness.test_spec.test_spec import make_test_spec
spec = make_test_spec(instance)
print(spec.instance_image_key, spec.FAIL_TO_PASS)
```

Selection rules learned the hard way:

- **Prefer `len(PASS_TO_PASS) == 0` and one `FAIL_TO_PASS`** for a smoke run; the
  grading step is minutes instead of an hour.
- **Reject instances whose `eval_script` resets the whole repo.** It should read
  `git checkout <base_commit> <test files...>`. When the test patch only *adds*
  files, `get_modified_files()` can return empty and the command degenerates to a
  bare `git checkout <base_commit>`, which reverts the model patch — such
  instances can never resolve. Check before you burn a run:
  ```python
  reset = [l for l in spec.eval_script.splitlines()
           if l.strip().startswith(f"git checkout {instance['base_commit']}")]
  assert all(len(l.strip().split()) > 3 for l in reset), "unscoped reset"
  ```
- Public per-instance images are large (~1 GB) but pull fast on clusters with
  image acceleration; confirm the exact repository name exists before assuming a
  naming convention.

## Step 6 — Task payload

```jsonc
{
  "task_id": "swe-<instance>-<attempt>",
  "instruction": "<instance.problem_statement>",
  "num_samples": 1,
  "timeout_seconds": 3000,
  "runtime": { /* Step 3 */ },
  "agent": {"harness": "mini_swe_agent", "model_name": "<served-model-name>"},
  "builder": {"strategy": "per_request"},
  "evaluator": {
    "strategy": "swebench_harness",
    "refresh_runtime": true,
    "config": {
      "repo_dir": "/testbed",
      "patch_command": "cd /polar/session/workspace && git add -A && git diff --cached --binary",
      "instance": { /* the raw dataset row */ },
      "apply_timeout": 300,
      "test_timeout": 1800
    }
  }
}
```

Notes:

- `timeout_seconds` is the **whole-session** budget; INIT, RUN and POST_RUN all
  draw from it. A slow harness install can consume half of it.
- `refresh_runtime: true` grades in a second, clean sandbox. The agent must
  therefore work in a *copy* of the repo (`/polar/session/workspace`), leaving
  `repo_dir` pristine for `git apply`.
- `patch_command` runs in the **source** runtime; `repo_dir` is where the patch is
  applied and tested in the **eval** runtime.
- `task_id` is the idempotency key. Resubmitting the same id returns the existing
  task instead of starting a new one — bump it per attempt.

## Step 7 — Submit and watch

```bash
curl -sX POST http://<control-host>:8080/rollout/task/submit \
  -H 'Content-Type: application/json' --data @task.json
curl -s http://<control-host>:8080/rollout/task/<task_id> | jq .status
```

Session stages are `INITIALIZING → READY → RUNNING → POST_RUN → COMPLETED`.
Watch the pieces that actually tell you where it is:

```bash
kubectl -n <namespace> get sandboxclaim,sandbox -w     # claim -> Completed, sandbox bound
kubectl -n <namespace> exec <claimed-sandbox> -- sh -c 'ls /polar/session/workspace | wc -l'
tail -f gateway.log    # proxied LLM calls appear as "POST /v1/chat/completions"
```

## Step 8 — Verification checklist

A run is genuinely green when **all** of these hold:

- [ ] `status == "COMPLETED"` and `error is None`.
- [ ] `trajectory.metadata.evaluation.outcome_reward == 1.0` and
      `report.resolved == true`, with `FAIL_TO_PASS.success` non-empty.
- [ ] `report.failed_apply_patch == false` and `apply_patch_output` contains
      `__POLAR_APPLY_PATCH_PASS__` — this is the proof that the patch reached the
      *remote* eval sandbox.
- [ ] `trajectory.traces` is non-empty and each trace carries `prompt_ids`,
      `response_ids` and `response_logprobs` — proof the proxy captured and
      canonicalized the calls.
- [ ] Completion records were persisted under `save_dir`.
- [ ] Teardown left nothing behind (Step 9).

## Step 9 — Teardown

`stop()` deletes the claimed Sandbox **and** its SandboxClaim, and deliberately
keeps the warm pool. Confirm, don't assume:

```bash
kubectl -n <namespace> get sandboxclaim        # expect: none of yours
kubectl -n <namespace> get sandbox -L agents.kruise.io/sandbox-claimed
kubectl -n <namespace> delete sandboxset <pool-name>   # when done for good
```

A sandbox stuck at `sandbox-claimed=true` whose claim is gone is an orphan: the
controller does **not** release a claimed sandbox when its claim is deleted, and
the pool will not reclaim it. Delete it by hand and treat it as a bug report —
it means the runtime resolved the wrong sandbox (see Troubleshooting).

## Runtime contract

What every backend guarantees, and what remote backends do differently:

| Member | Remote behaviour |
| --- | --- |
| `start()` | Provisions the sandbox and creates `/polar/session/{artifacts,logs/agent,logs/eval,eval_artifacts}` inside it. |
| `exec(cmd, cwd, env, timeout_sec)` | `timeout_sec` expiry returns `ExecResult(return_code=-1)`, never raises. `spec.env` is merged into every call. |
| `upload_file` / `upload_dir` | Stream a tar over the exec channel (ACK) or the files API (E2B). |
| `download_file` / `download_dir` | Inverse; ACK reads the stream in **binary** mode — text mode decodes each websocket frame as UTF-8 and silently corrupts binary payloads. |
| `resolve_host_path()` | Always `None`. There is no host path. |
| `publish_file(local, runtime_path)` | No-op for bind-mounted backends; uploads for remote ones. **Use this instead of writing to the host session dir.** |
| `stop()` | Idempotent; deletes the sandbox and claim, keeps the pool. |

Internal bookkeeping commands (`mkdir -p`, session bootstrap) must pin
`cwd="/"`. They run before `spec.workdir` exists, and a failed `cd` aborts the
command before it executes.

Direct SDK use, without the rollout/gateway services:

```python
from pathlib import Path
from polar.runtime.factory import create_runtime
from polar.runtime.models import RuntimeSpec

spec = RuntimeSpec(backend="ack", image="<image>", kwargs={"namespace": "<namespace>"})
runtime = create_runtime(spec, "session-1", Path("/tmp/session-1"))
await runtime.start()
result = await runtime.exec("python -V", timeout_sec=30)
await runtime.publish_file(Path("eval.sh"), f"{runtime.runtime_session_dir}/eval.sh")
await runtime.stop()
```

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Session runs, but a *different* sandbox shows the work; a claimed sandbox is orphaned after teardown | Claim→sandbox resolution picked an arbitrary pool member | Resolve by the `agents.kruise.io/claim-name` **label**; never fall back to an unmatched list |
| `failed to create /polar/session ... can't cd to <workdir>` | Internal exec inherited `spec.workdir`, which does not exist yet | Pin `cwd="/"` for bootstrap/mkdir commands |
| Evaluator reports `failed_apply_patch`, or `eval.sh: No such file or directory` | File was written to the host session dir; no bind mount | Stage it with `publish_file()` |
| `git config --global` exits 255 in `eval_prepare` | `$HOME` points into `/polar/session` and was never created | `mkdir -p "$HOME"` first |
| `prepare` exits 127 on the harness CLI | `PATH` lacks the install prefix at INIT time | Export `PATH` in `prepare`, or call the absolute path |
| `ModuleNotFoundError: swebench.harness.test_spec` | `swebench` 5.x installed | Pin `swebench>=4,<5`; **restart the gateway** afterwards — a failed import is cached in `sys.modules` for the life of the process |
| `cannot import name 'DEFAULT_DOCKER_SPECS'` right after downgrading | Leftover 5.x package directory shadows the 4.x module | Uninstall, delete `site-packages/swebench*`, reinstall |
| Claim stuck in `Claiming` | Pool cannot schedule: image pull, quota, taints, or provider-specific resource params | `kubectl describe sandboxclaim` / `sandbox`; check node taints and provider annotations |
| `reward == 0` with a correct-looking patch | Instance's `eval_script` resets the whole repo | Re-pick the instance (Step 5) |
| Binary artifacts come back mangled | Exec stream decoded as text | Read the stream with `binary=True` |
| Agent cannot reach the model | `gateway.nodes[].public_url` unreachable from the sandbox | Test from a pod in the cluster, not from your laptop |

## Appendix A: smoke-testing without a model server

To validate the runtime, proxy and evaluator without GPUs or an API key, stand in
a stub that speaks the engine's dialect. The gateway always POSTs
`{inference.base_url}/v1/chat/completions` with `stream=false` and the params the
engine strategy injects — for `sglang`: `logprobs`, `return_prompt_token_ids`,
`return_meta_info`.

The stub must answer in the shape `SGLangEngine.normalize_response` expects:

```jsonc
{"choices": [{
  "message": {"role": "assistant", "content": "...",
              "tool_calls": [{"id": "call_1", "type": "function",
                              "function": {"name": "bash",
                                           "arguments": "{\"command\": \"...\"}"}}]},
  "finish_reason": "tool_calls",
  "prompt_token_ids": [/* ints */],
  "logprobs": {"content": [{"token": "t", "token_id": 1, "logprob": -0.01}]},
  "meta_info": {"output_token_logprobs": [[-0.01, 1, "t"]]}
}]}
```

Derive the step from the conversation (count prior `assistant` messages) so the
stub stays stateless. The gateway forwards no `Authorization` header upstream, so
the session id is not available to it.

This exercises everything except neural inference: the real harness runs, its
tool calls execute in the sandbox, the proxy captures and canonicalizes both
directions, and grading is real. A stub returning a known-good patch should
produce `outcome_reward == 1.0`; anything less is a pipeline defect, not a model
quality issue. Label results from a stub run as such.

## Appendix B: one-command sanity check

Before wiring a full evaluation, prove the backend in isolation:

```bash
python - <<'PY'
import asyncio
from pathlib import Path
from polar.runtime.factory import create_runtime
from polar.runtime.models import RuntimeSpec

async def main():
    spec = RuntimeSpec(backend="ack", image="<tiny-image>",
                       kwargs={"namespace": "<namespace>", "use_sandbox_claim": True,
                               "sandboxset_replicas": 1})
    rt = create_runtime(spec, "sanity", Path("/tmp/sanity"))
    await rt.start()
    print("id       :", rt.runtime_id)
    print("exec     :", (await rt.exec("echo hi")).stdout.strip())
    print("timeout  :", (await rt.exec("sleep 5", timeout_sec=1)).return_code)  # -1
    print("host path:", rt.resolve_host_path("/polar/session"))                 # None
    await rt.stop()

asyncio.run(main())
PY
```
