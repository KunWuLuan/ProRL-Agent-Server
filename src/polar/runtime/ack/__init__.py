"""Kubernetes-backed rollout runtime (Alibaba Cloud ACK, or any kubeconfig cluster).

Migrated from Harbor's ``ACKEnvironment`` onto Polar's runtime contract: one Pod
per session, shared by the init → run → eval stages.

Two allocation modes:

- **Pod mode** (default) — create one ``sleep infinity`` Pod from ``spec.image``.
- **SandboxClaim mode** (``kwargs.use_sandbox_claim``) — claim a pre-warmed
  sandbox from an OpenKruise ``SandboxSet`` pool. Session start is much faster,
  which matters when a gateway node dispatches many rollouts at once.

Images must already be built and pushed. Unlike Harbor's ``ACKEnvironment``,
this runtime never builds from a Dockerfile: ``RuntimeSpec.image`` is a prebuilt
reference here, exactly as it is for the Docker and Apptainer backends.

Config (``RuntimeSpec``)
------------------------
- ``image`` — container image for the Pod / SandboxSet template.
- ``env`` — merged into every ``exec``.
- ``cpus`` / ``memory_mb`` / ``storage_mb`` — Pod resource *requests*
  (memory and storage in ``Mi``; storage maps to ``ephemeral-storage``).
- ``kwargs.namespace`` *(str, required)* — namespace for Pods and claims.
- ``kwargs.context`` / ``kwargs.kubeconfig`` — cluster selection; falls back to
  in-cluster config when no kubeconfig is reachable.
- ``kwargs.image_pull_secret`` / ``kwargs.service_account`` — pull auth and SA.
- ``kwargs.node_selector`` *(dict)* / ``kwargs.tolerations`` *(list)* — scheduling.
- ``kwargs.pod_overrides`` *(dict)* — deep-merged into the Pod (or SandboxSet
  template) manifest, mirroring the Kubernetes structure.
- ``kwargs.memory_limit_multiplier`` *(float)* — set a memory *limit* of
  ``memory_mb * multiplier`` on top of the request.
- ``kwargs.pod_ready_timeout`` *(int, default 300)* — seconds to wait for ready.
- ``kwargs.user`` *(str | int)* — run commands through ``su`` as this user.
- ``kwargs.use_sandbox_claim`` *(bool, default False)* — enable pool mode.
- ``kwargs.sandboxset_name`` *(str)* — pool name; derived from ``image`` by default.
- ``kwargs.sandbox_image`` *(str)* — pool image; defaults to ``spec.image``.
- ``kwargs.sandboxset_replicas`` *(int, default 5)* — warm pool size.
- ``kwargs.claim_timeout`` *(int, default 300)* — seconds to wait for a claim.
- ``kwargs.sandbox_labels`` / ``kwargs.sandbox_annotations`` *(dict)* — pool metadata.
- ``kwargs.sandbox_env_vars`` *(dict)* — env vars carried on the claim. The
  controller only injects these when the pool has envd enabled, so prefer
  ``spec.env`` (merged into every ``exec``) for per-session variables.

Requires the ``ack`` extra: ``uv pip install 'polar[ack]'``.
"""

from polar.runtime.ack._sdk import (
    _CLAIM_NAME_LABEL,
    _POD_LABELS,
    _SANDBOX_API_VERSION,
)
from polar.runtime.ack.client import KubernetesClientManager
from polar.runtime.ack.runtime import ACKRuntime

__all__ = [
    "ACKRuntime",
    "KubernetesClientManager",
    "_CLAIM_NAME_LABEL",
    "_POD_LABELS",
    "_SANDBOX_API_VERSION",
]
