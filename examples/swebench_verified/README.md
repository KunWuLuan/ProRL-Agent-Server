# SWE-bench Verified Example

Evaluate Polar agent harnesses on [SWE-bench Verified](https://huggingface.co/datasets/princeton-nlp/SWE-bench_Verified)
(500 human-validated tasks). Each task runs an agent inside a per-instance
container at the repo's `base_commit`, then grades the patch with the official
`swebench` harness.

## Prerequisites

Install **Polar** and one inference backend — **vLLM** or **SGLang** — as
described in the [top-level README](../../README.md#installation). This example
also needs Polar's optional **SWE-bench** extra (the official grading harness
the evaluator runs):

```bash
uv pip install -e ".[swebench]"
```

This example assumes 1 node **8×H100** — two inference servers (tensor-parallel 4 each).

Adjust the setup and topology for your hardware.

## Quick Start

### 1. Build runtime images

Each runtime image layers Node.js on the per-instance SWE-bench image; harness
CLIs install at task time during the **INIT** stage. Build a subset first:

```bash
uv run python examples/swebench_verified/build_images.py --max-tasks 10   # or no flag for all 500
```

### 2. Start two inference servers

Pick **one** backend (don't install both in the same environment).

**vLLM** → use `topology.vllm.yaml`

```bash
CUDA_VISIBLE_DEVICES=0,1,2,3 uv run vllm serve Qwen/Qwen3.6-27B --port 8000 \
  --tensor-parallel-size 4 --max-model-len 262144 \
  --reasoning-parser qwen3 --enable-auto-tool-choice --tool-call-parser qwen3_coder

CUDA_VISIBLE_DEVICES=4,5,6,7 uv run vllm serve Qwen/Qwen3.6-27B --port 8001 \
  --tensor-parallel-size 4 --max-model-len 262144 \
  --reasoning-parser qwen3 --enable-auto-tool-choice --tool-call-parser qwen3_coder
```

**SGLang** → use `topology.sgl.yaml`

```bash
CUDA_VISIBLE_DEVICES=0,1,2,3 uv run python -m sglang.launch_server --model-path Qwen/Qwen3.6-27B --port 8000 \
  --tp 4 --context-length 262144 --mem-fraction-static 0.85 \
  --reasoning-parser qwen3 --tool-call-parser qwen3_coder

CUDA_VISIBLE_DEVICES=4,5,6,7 uv run python -m sglang.launch_server --model-path Qwen/Qwen3.6-27B --port 8001 \
  --tp 4 --context-length 262144 --mem-fraction-static 0.85 \
  --reasoning-parser qwen3 --tool-call-parser qwen3_coder
```

### 3. Start Polar

Use the topology file that matches your backend (`topology.vllm.yaml` shown; swap for `topology.sgl.yaml`):

```bash
uv run polar serve_rollout -c examples/swebench_verified/topology.vllm.yaml
uv run polar serve_gateway -c examples/swebench_verified/topology.vllm.yaml --node-id localhost-node-01
uv run polar serve_gateway -c examples/swebench_verified/topology.vllm.yaml --node-id localhost-node-02
```

### 4. Submit tasks

Pick a harness and how many tasks to run; the resolved-rate summary prints to
the console when the batch finishes. Supported harnesses: `claude_code`, `codex`, `opencode`, `qwen_code`, `mini_swe_agent`.


```bash
# pass@1 over the first 10 tasks
uv run python examples/swebench_verified/submit_swebench_tasks.py --harness claude_code --max-tasks 10

# pass@8 over the first 10 tasks
uv run python examples/swebench_verified/submit_swebench_tasks.py --harness claude_code --max-tasks 10 --num-samples 8

# a single instance
uv run python examples/swebench_verified/submit_swebench_tasks.py --harness codex --instance-id django__django-15098
```

Use Apptainer instead of Docker with `--runtime-backend apptainer`.

### 5. (Optional) Watch in the dashboard

`topology.vllm.yaml` shown; swap for `topology.sgl.yaml` if you use sglang.

```bash
uv run polar dashboard -c examples/swebench_verified/topology.vllm.yaml
```

Open <http://127.0.0.1:8090> for per-task patches, trajectories, and grading.

## Remote sandbox backends (E2B, ACK)

Skip `build_images.py` when the per-instance images already live in a registry the
cluster can pull: the remote backends never build, they launch `--image-template`
against a sandbox pool. `--runtime-kwargs template=<pool>` names that pool, and
`--image-template` maps each instance to its registry reference (`{image_key}` is
the `swebench`-derived image name, `{instance_id}` / `{slug}` are also available).

```bash
uv run python examples/swebench_verified/submit_swebench_tasks.py \
  --harness mini_swe_agent --instance-id pylint-dev__pylint-4661 \
  --runtime-backend e2b --runtime-kwargs template=<pool> \
  --image-template "docker.io/{image_key}" --topology <topology.yaml>
```

Keep the default `--refresh-runtime`: grading applies the extracted patch to a
pristine `/testbed` in a **second** sandbox, so `--no-refresh-runtime` grades an
untouched repo and reports reward `0.0` with no error. Pool setup, the E2B SDK
environment (`E2B_API_KEY` / `E2B_API_URL` / `E2B_SANDBOX_URL`) and the
verification checklist are in the
[remote quick start](../../src/polar/runtime/ack/QUICKSTART.md).

The `ack` backend runs the same two commands with `--runtime-backend ack` and its own
required kwargs — see [ACK → Running the shipped examples](../../src/polar/runtime/ack/README.md#running-the-shipped-examples).

