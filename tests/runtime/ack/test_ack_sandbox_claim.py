"""Sandbox-claim resolution for the ACK runtime.

The controller records which pool member a ``SandboxClaim`` took in the
``agents.kruise.io/claim-name`` **label** on the Sandbox; ``SandboxClaim.status``
only carries a replica count. Resolving anything else runs the session in a
sandbox the claim does not own — and because the controller does not release a
claimed Sandbox when its claim is deleted, teardown then orphans the real one.
These tests pin both the selector and the no-match behaviour.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("kubernetes", reason="the ack extra is not installed")

from polar.runtime.ack import _CLAIM_NAME_LABEL, ACKRuntime  # noqa: E402
from polar.runtime.models import RuntimeSpec  # noqa: E402

CLAIM = "polar-session-1-cl-abcdef01"
CLAIMED = "agents.kruise.io/sandbox-claimed"


class _Metadata:
    def __init__(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.labels = labels or {}
        self.annotations = annotations or {}


class _Sandbox:
    def __init__(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
    ) -> None:
        self.metadata = _Metadata(name, labels, annotations)


class _SandboxList:
    def __init__(self, items: list[Any]) -> None:
        self.items = items


class _FakeSandboxApi:
    """Stands in for the DynamicClient resource API used by ``_get_claimed_sandbox``."""

    def __init__(self, sandboxes: list[_Sandbox]) -> None:
        self._sandboxes = sandboxes
        self.selectors: list[str | None] = []

    def get(
        self,
        namespace: str | None = None,
        label_selector: str | None = None,
        name: str | None = None,
    ) -> _SandboxList:
        self.selectors.append(label_selector)
        if label_selector is None:
            return _SandboxList(list(self._sandboxes))
        key, _, value = label_selector.partition("=")
        return _SandboxList(
            [
                sandbox
                for sandbox in self._sandboxes
                if (sandbox.metadata.labels or {}).get(key) == value
            ]
        )


def _runtime(api: _FakeSandboxApi) -> ACKRuntime:
    spec = RuntimeSpec(
        backend="ack",
        image="registry.invalid/pool:latest",
        kwargs={"namespace": "polar-runtime", "use_sandbox_claim": True},
    )
    runtime = ACKRuntime(spec, "session-1", Path("/tmp/polar-ack-claim-test"))
    runtime._sandbox_api = api
    runtime._claim_name = CLAIM
    return runtime


def _pool() -> list[_Sandbox]:
    """Two pool members plus the one our claim actually took (not first in list)."""
    return [
        _Sandbox("pool-aaa", {_CLAIM_NAME_LABEL: "some-other-claim", CLAIMED: "true"}),
        _Sandbox("pool-bbb", {CLAIMED: "false"}),
        _Sandbox("pool-ccc", {_CLAIM_NAME_LABEL: CLAIM, CLAIMED: "true"}),
    ]


def test_resolves_the_sandbox_bound_to_the_claim() -> None:
    api = _FakeSandboxApi(_pool())
    runtime = _runtime(api)

    asyncio.run(runtime._get_claimed_sandbox())

    assert runtime._sandbox_name == "pool-ccc"
    assert runtime._pod_name == "pool-ccc"
    assert api.selectors == [f"{_CLAIM_NAME_LABEL}={CLAIM}"]


def test_unbound_claim_raises_instead_of_picking_a_pool_member() -> None:
    sandboxes = _pool()
    for sandbox in sandboxes:
        sandbox.metadata.labels.pop(_CLAIM_NAME_LABEL, None)
    runtime = _runtime(_FakeSandboxApi(sandboxes))

    with pytest.raises(RuntimeError, match="no Sandbox is bound"):
        asyncio.run(runtime._get_claimed_sandbox())

    assert runtime._sandbox_name is None
    assert runtime._pod_name != "pool-aaa"


def test_scans_labels_when_the_selector_is_not_indexed() -> None:
    class _UnindexedApi(_FakeSandboxApi):
        def get(
            self,
            namespace: str | None = None,
            label_selector: str | None = None,
            name: str | None = None,
        ) -> _SandboxList:
            self.selectors.append(label_selector)
            if label_selector is not None:
                return _SandboxList([])
            return _SandboxList(list(self._sandboxes))

    runtime = _runtime(_UnindexedApi(_pool()))

    asyncio.run(runtime._get_claimed_sandbox())

    assert runtime._sandbox_name == "pool-ccc"


def test_annotation_binding_is_honoured_by_the_scan() -> None:
    class _UnindexedApi(_FakeSandboxApi):
        def get(
            self,
            namespace: str | None = None,
            label_selector: str | None = None,
            name: str | None = None,
        ) -> _SandboxList:
            if label_selector is not None:
                return _SandboxList([])
            return _SandboxList(list(self._sandboxes))

    sandboxes = [
        _Sandbox("pool-aaa", {CLAIMED: "false"}),
        _Sandbox("pool-bbb", {CLAIMED: "true"}, {_CLAIM_NAME_LABEL: CLAIM}),
    ]
    runtime = _runtime(_UnindexedApi(sandboxes))

    asyncio.run(runtime._get_claimed_sandbox())

    assert runtime._sandbox_name == "pool-bbb"
