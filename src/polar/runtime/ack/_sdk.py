"""Optional-dependency guard and cluster constants for the ACK backend.

Importing this module is what enforces the ``ack`` extra: the imports below
raise a readable ``RuntimeError`` when ``kubernetes`` or ``tenacity`` is missing.
Every other module in the package takes its SDK names from here, so the guard
runs exactly once and the failure message is the same everywhere.
"""

from __future__ import annotations

try:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.client.rest import ApiException
    from kubernetes.dynamic import DynamicClient
    from kubernetes.stream import stream
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError as exc:
    raise RuntimeError(
        "the 'ack' runtime backend requires the ack extra: uv pip install 'polar[ack]'"
    ) from exc

_SANDBOX_API_VERSION = "agents.kruise.io/v1alpha1"
_POD_LABELS = {"app": "polar-sandbox", "backend": "ack"}
# The controller records the claim->sandbox binding on the Sandbox itself, in
# this label. SandboxClaim.status only carries a replica count, so the label is
# the only way to tell which pool member a claim actually took.
_CLAIM_NAME_LABEL = "agents.kruise.io/claim-name"

__all__ = [
    "ApiException",
    "DynamicClient",
    "_CLAIM_NAME_LABEL",
    "_POD_LABELS",
    "_SANDBOX_API_VERSION",
    "k8s_client",
    "k8s_config",
    "retry",
    "stop_after_attempt",
    "stream",
    "wait_exponential",
]
