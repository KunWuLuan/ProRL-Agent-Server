# Configuration

`polar.config` loads and validates the single `topology.yaml` that describes a
whole Polar deployment: one **rollout** server plus one or more **gateway**
nodes (each with its own inference backend). `TopologyConfig.load()` is the entry
point every `polar` command uses.

## Mental model

One file, two halves:

- `rollout:` — the central orchestrator that clients submit tasks to.
- `gateway:` — the worker fleet. `gateway.nodes[]` is a list; each entry is an
  independent gateway process with its own ports, worker pools, and inference
  endpoint.

The schema is **strict and immutable**: unknown keys are rejected
(`extra="forbid"`, so a typo fails fast) and every model is frozen after load.
Convenience defaulting fills the gaps — a blank `public_url` is derived from
`host:port` (mapping `0.0.0.0`/`::` → `127.0.0.1`), and `gateway.rollout_server_url`
falls back to `rollout.public_url` when omitted.
Those derived loopback URLs are **single-host test defaults**: right for a
laptop run where every process shares one network namespace, wrong for any
deployment where a caller lives somewhere else — notably a remote sandbox, which
dials the gateway's `public_url` from inside another pod
([network addressing](../runtime/ack/RUNBOOK.md#network-addressing-who-must-reach-whom)).

## Main files

- `topology.py`: the Pydantic models (`TopologyConfig`, `RolloutServiceConfig`,
  `GatewayConfig`, `GatewayNodeConfig`), `load()`, and the URL/selection helpers.
- `__init__.py`: package exports.

## Schema

**`rollout`** — `RolloutServiceConfig`

| field | type | default |
|---|---|---|
| `host` | str | `0.0.0.0` |
| `port` | int | `8080` |
| `public_url` | str | derived from `host:port` |
| `save_dir` | str? | `None` (no result persistence) |
| `dispatch_poll_interval_seconds` | float | `1.0` |
| `callback_grace_seconds` | float | `120.0` |

**`gateway`** — `GatewayConfig`

| field | type | default |
|---|---|---|
| `heartbeat_interval_seconds` | int | `30` |
| `rollout_server_url` | str? | `rollout.public_url` |
| `nodes` | list | **required**, ≥1, unique ids |
| `completion_persistence` | block | `enabled` |

`completion_persistence` controls the async on-disk capture of model calls:
`enabled` (`true`), `max_field_bytes` (`1048576`), `queue_size` (`1024`).

**`gateway.nodes[]`** — `GatewayNodeConfig`

| field | type | default |
|---|---|---|
| `id` | str | hostname (must be unique) |
| `host` / `port` | str / int | `0.0.0.0` / `8081` |
| `public_url` | str | derived from `host:port` |
| `model_served` | str | `""` |
| `inference.engine` | `sglang` \| `vllm` | `sglang` |
| `inference.base_url` | str | `http://127.0.0.1:8000` |
| `max_init_workers` | int | `4` |
| `max_run_workers` | int | `2` |
| `max_postrun_workers` | int | `4` |
| `default_runtime` | `RuntimeSpec`? | `None` |

## Example

```yaml
rollout:
  host: 127.0.0.1
  port: 8080
  public_url: http://127.0.0.1:8080
  save_dir: ./rollout_results

gateway:
  heartbeat_interval_seconds: 30
  nodes:
    - id: localhost-node-01
      host: 127.0.0.1
      port: 8100
      public_url: http://127.0.0.1:8100
      model_served: Qwen/Qwen3.5-4B
      max_init_workers: 8
      max_run_workers: 4
      max_postrun_workers: 4
      inference:
        engine: sglang   # or vllm
        base_url: http://127.0.0.1:8000
```

The example is a single-host development topology: every URL is loopback because
every process shares the host. A cluster deployment advertises resolvable
addresses instead — see
[Step 4](../runtime/ack/RUNBOOK.md#step-4--deploy-the-control-plane-where-sandboxes-can-reach-it).

## Reachable URLs and multi-node

`public_url`s must be reachable by whoever calls them: the rollout server calls
each node's `public_url`; each node calls back to `rollout_server_url` and its
own `inference.base_url`. A remote sandbox runtime adds a fourth caller: the
agent inside the sandbox, which receives the node's `public_url` as its
`OPENAI_BASE_URL` / `ANTHROPIC_BASE_URL` / `GOOGLE_API_URL`.

Locally the derived `127.0.0.1` URLs work, because every caller shares the host.
Treat them as **test-only** anywhere else: for multi-host deployments — and for
every run whose sandbox is a remote pod — set explicit addresses each caller can
resolve. `inference.base_url` is the exception, since loopback stays correct when
the inference engine runs beside the gateway.

`polar serve_gateway` requires `--node-id` when the topology has more than one
node, so a gateway process always starts with the right ports, worker limits,
and inference endpoint.
