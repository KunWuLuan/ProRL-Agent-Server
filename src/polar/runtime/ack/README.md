# ACK Runtime Backend

Kubernetes-backed rollout runtime: one Pod — or one claimed OpenKruise sandbox —
per session. Works against any cluster reachable through a kubeconfig context or
an in-cluster ServiceAccount.

This is the backend-specific reference. For the end-to-end procedure that applies
to every remote backend (control-plane placement, task payload, verification,
RL training), see [the runbook](../RUNBOOK.md).

## Package layout

| Module | Contents |
| --- | --- |
| `__init__.py` | Package docstring with the full `RuntimeSpec` config reference; re-exports `ACKRuntime`. |
| `_sdk.py` | The `polar[ack]` extra guard (`kubernetes`, `tenacity`) and cluster constants. Importing it is what raises the readable error when the extra is missing. |
| `_util.py` | Pure helpers: `pod_overrides` deep-merge, kwargs coercion, DNS-1123 name and label-value derivation. |
| `client.py` | `KubernetesClientManager` — one reference-counted client per process, closed at interpreter exit. |
| `runtime.py` | `ACKRuntime` — lifecycle, exec, file transfer, Pod readiness, and the OpenKruise pool path. |

The factory resolves the backend lazily as `polar.runtime.ack:ACKRuntime`, so a
base install never imports the Kubernetes SDK.

## Allocation modes

| Mode | `kwargs` | Start latency | Scales to many distinct images | Use when |
| --- | --- | --- | --- | --- |
| Pod (default) | — | image pull + schedule | yes | one-off runs, no OpenKruise in the cluster, per-instance images |
| SandboxClaim | `use_sandbox_claim: true` | seconds (warm pool) | no — one pool per image | many concurrent rollouts over a **small** set of images |

**Pod mode** creates a `sleep infinity` Pod named after the session, with
`restartPolicy: Never`, and deletes it on `stop()`. The container runs
`privileged: true` as user 0 so harnesses can install packages and mount
workspaces; use `kwargs.pod_overrides` to tighten that if your cluster policy
requires it.

**SandboxClaim mode** keeps a warm `SandboxSet` pool and claims one member per
session, which avoids image pull and scheduling on the critical path. See
[OpenKruise pool](#openkruise-sandboxset--sandboxclaim) for the lifecycle and its
sharp edges.

## RBAC

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
`RuntimeError` from `ACKRuntime.start()`, and a 403 on claim creation is reported
explicitly as a missing `agents.kruise.io` permission.

## Configuration

Generic `RuntimeSpec` fields behave as documented in
[the runbook](../RUNBOOK.md#step-3--runtime-spec). ACK-specific `kwargs`:

| `kwargs` | Default | Notes |
| --- | --- | --- |
| `namespace` | *required* | Namespace for Pods, claims and pools. |
| `context` / `kubeconfig` | in-cluster | Cluster selection. All runtimes **in one process must use the same context** — the client manager is a singleton and raises on a mismatch. |
| `image_pull_secret` | — | Added to `imagePullSecrets`. |
| `service_account` | — | `serviceAccountName` for the Pod / pool template. |
| `node_selector` / `tolerations` | — | Scheduling constraints. |
| `pod_overrides` | — | Deep-merged into the Pod (or pool template) manifest, mirroring the Kubernetes structure. `containers` / `initContainers` merge element-wise by index; everything else recurses by key. This is also how you attach a real volume (see below). |
| `cpus` / `memory_mb` / `storage_mb` | — | Become resource **requests** (`memory` and `ephemeral-storage` in `Mi`). |
| `memory_limit_multiplier` | — | Adds a memory *limit* of `memory_mb × multiplier` on top of the request. |
| `pod_ready_timeout` | 300 | Seconds to wait for the Pod to become ready. |
| `user` | — | Run every command through `su` as this user. |
| `use_sandbox_claim` | false | Enable pool mode. |
| `sandbox_image` | `spec.image` | Pool image, when it should differ from the session image. |
| `sandboxset_name` | derived from the image | Pin only if you also manage the image yourself — see the warning below. |
| `sandboxset_replicas` | 5 | Warm pool size. |
| `claim_timeout` | 300 | Seconds to wait for a claim to complete. |
| `sandbox_labels` / `sandbox_annotations` | — | Extra metadata on the pool and the claim. |
| `sandbox_env_vars` | — | Env vars carried on the claim. The controller only injects them into pools with envd enabled — prefer `spec.env`, which ACK merges into every `exec`. |

### No bind mount, so no `kwargs.volumes`

`kwargs.volumes` is read only by the `docker` and `apptainer` backends and is
**silently ignored** here; `factory.py` has no capability check for it. A config
ported from a bind-mounted backend therefore loses its mounted tooling with no
error, and the harness CLI is not found at RUN.

Either bake the tooling into the image, install it in `prepare`, or express a real
mount through `pod_overrides`:

```yaml
kwargs:
  pod_overrides:
    spec:
      volumes:
        - name: tools
          persistentVolumeClaim: {claimName: <pvc>}
      containers:
        - volumeMounts: [{name: tools, mountPath: /opt/tools, readOnly: true}]
```

`containers` merges by index, so the single entry above lands on the runtime's
`main` container.

## OpenKruise SandboxSet / SandboxClaim

### Pool creation

`_ensure_sandboxset()` creates the pool on first use and returns immediately if a
`SandboxSet` with that name already exists. The pool template mirrors the Pod
spec used in standard mode, so `pod_overrides`, scheduling and resources apply to
both modes. Pools are **shared across sessions and deliberately left running on
`stop()`**.

The pool name defaults to a hash of the image (`polar-pool-<slug>-<digest>`), and
the image is also recorded as an `image` label on the pool.

> **Pinning `sandboxset_name` defeats image replacement.** Because an existing
> pool is reused without comparing images, changing `spec.image` while keeping a
> pinned pool name silently runs the *old* image, with nothing in the logs to say
> so. Either let the name be derived, or rename the pool together with the image.

### Claim lifecycle

Each session creates one `SandboxClaim` with `replicas: 1`,
`claimTimeout: "<claim_timeout>s"` and `createOnNoStock: true`. The last flag
means an exhausted pool degrades to on-demand creation instead of failing — so an
under-sized pool never errors, it just loses the warm-start benefit precisely at
peak load. A 409 on create deletes the stale claim and recreates it.

### Claim → sandbox resolution

`SandboxClaim.status` carries only a replica count, so it cannot tell you *which*
pool member was taken. The controller records the binding on the Sandbox itself in
the `agents.kruise.io/claim-name` label; the runtime selects on exactly that
label and raises rather than falling back to an unmatched list.

Resolving anything else runs the session in a sandbox the claim does not own —
and because the controller does not release a claimed Sandbox when its claim is
deleted, teardown then orphans the real one. `tests/runtime/` pins both the
selector and the no-match behaviour.

### Sizing the pool

One session is one sandbox, and `evaluator.refresh_runtime: true` adds a second
clean sandbox for grading. Under RL training the peak is
`max_session_concurrency` — see
[the runbook's sizing section](../RUNBOOK.md#sizing-the-sandbox-pool-for-training-concurrency).
Set `sandboxset_replicas` at or above that per distinct image, and raise the
gateway node's `max_*_workers` to match, or a large pool sits idle behind small
worker limits.

## Observability

```bash
kubectl -n <namespace> get sandboxclaim,sandbox -w     # claim -> Completed, sandbox bound
kubectl -n <namespace> exec <claimed-sandbox> -- sh -c 'ls /polar/session/workspace | wc -l'
kubectl -n <namespace> describe sandboxclaim <claim>   # when a claim sticks in Claiming
```

## Teardown

`stop()` is idempotent and deletes the claimed Sandbox **and** its SandboxClaim,
keeping the warm pool. In Pod mode it deletes the Pod. Confirm, don't assume:

```bash
kubectl -n <namespace> get sandboxclaim        # expect: none of yours
kubectl -n <namespace> get sandbox -L agents.kruise.io/sandbox-claimed
kubectl -n <namespace> delete sandboxset <pool-name>   # when done for good
```

A sandbox stuck at `sandbox-claimed=true` whose claim is gone is an orphan: the
controller does **not** release a claimed sandbox when its claim is deleted, and
the pool will not reclaim it. Delete it by hand and treat it as a bug report — it
means the runtime resolved the wrong sandbox.

Because pools outlive sessions, a run over a per-instance-image dataset leaves one
`SandboxSet` per image behind. List and delete them when the job finishes.

## Sanity check

Prove the backend in isolation before wiring a full evaluation:

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

## Troubleshooting

Backend-agnostic symptoms (missing files in the sandbox, `$HOME` and `PATH` in
`prepare`, `swebench` version conflicts, gateway reachability) are in
[the runbook](../RUNBOOK.md#troubleshooting). ACK-specific ones:

| Symptom | Cause | Fix |
| --- | --- | --- |
| `RuntimeError` from `start()`, 403 in the gateway log | Missing RBAC for `pods/exec` or the `agents.kruise.io` group | Apply the Role above; verify with `kubectl auth can-i` |
| Session runs, but a *different* sandbox shows the work; a claimed sandbox is orphaned after teardown | Claim→sandbox resolution picked an arbitrary pool member | Resolve by the `agents.kruise.io/claim-name` **label**; never fall back to an unmatched list |
| Claim stuck in `Claiming` | Pool cannot schedule: image pull, quota, taints, or provider-specific resource params | `kubectl describe sandboxclaim` / `sandbox`; check node taints and provider annotations |
| Session runs an **older** image than the spec says | `sandboxset_name` pinned while `image` changed; the existing pool is reused without an image check | Derive the name from the image, or rename the pool with the image |
| Hundreds of `SandboxSet`s left in the namespace | One pool per distinct image, and `stop()` keeps pools by design | Use Pod mode for per-instance-image datasets, or converge on one image |
| Warm pool exists but steps are still slow | `sandboxset_replicas` below peak concurrency; excess claims create on demand | Size the pool from the concurrency formula, and raise the node's `max_*_workers` |
| Pod stuck in `ImagePullBackOff` | Unpullable ref, or a missing `image_pull_secret` | `start()` raises with the kubelet message; verify the exact repository name exists |
| `KubernetesClientManager already initialized for context ...` | One process asked for two clusters | The client is a process-wide singleton; run one gateway process per cluster |
| Harness CLI not found at RUN after moving off Docker/Apptainer | `kwargs.volumes` is ignored here | Bake it into the image, install in `prepare`, or use `pod_overrides` |
