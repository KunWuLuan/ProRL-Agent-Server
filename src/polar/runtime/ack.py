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

from __future__ import annotations

import asyncio
import atexit
import hashlib
import io
import json
import logging
import re
import shlex
import sys
import tarfile
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Any

from polar.runtime.base import BaseRuntime, session_dirs_shell_command
from polar.runtime.models import ExecResult, RuntimeSpec

logger = logging.getLogger(__name__)

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


class KubernetesClientManager:
    """Share one Kubernetes client across every ACK runtime in the process.

    A gateway node drives many concurrent sessions against the same cluster, so
    the client is created once, reference-counted, and closed at interpreter
    exit. All runtimes in a process must use the same cluster context.
    """

    _instance: KubernetesClientManager | None = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._core_api: k8s_client.CoreV1Api | None = None
        self._api_client: k8s_client.ApiClient | None = None
        self._dynamic_client: DynamicClient | None = None
        self._reference_count = 0
        self._client_lock = asyncio.Lock()
        self._initialized = False
        self._cleanup_registered = False
        self._logger = logger.getChild("KubernetesClientManager")
        self._context: str | None = None
        self._kubeconfig: str | None = None

    @classmethod
    async def get_instance(cls) -> KubernetesClientManager:
        """Get or create the singleton instance."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        instance = cls._instance
        if instance is None:
            raise RuntimeError("KubernetesClientManager failed to initialize")
        return instance

    def _init_client(self, context: str | None, kubeconfig: str | None = None) -> None:
        """Initialize the Kubernetes client from kubeconfig, else in-cluster."""
        if self._initialized:
            return
        try:
            kwargs: dict[str, Any] = {}
            if context:
                kwargs["context"] = context
            if kubeconfig:
                kwargs["config_file"] = kubeconfig
            k8s_config.load_kube_config(**kwargs)
        except k8s_config.ConfigException as exc:
            try:
                k8s_config.load_incluster_config()
            except k8s_config.ConfigException as inner:
                raise RuntimeError(
                    f"failed to load kubeconfig: {inner}\n"
                    "Ensure kubectl is configured and can access the cluster."
                ) from exc
        self._api_client = k8s_client.ApiClient()
        self._core_api = k8s_client.CoreV1Api()
        self._dynamic_client = DynamicClient(self._api_client)
        self._initialized = True
        self._context = context
        self._kubeconfig = kubeconfig

    async def get_client(
        self, context: str | None = None, kubeconfig: str | None = None
    ) -> k8s_client.CoreV1Api:
        """Get the shared CoreV1Api client and increment the reference count."""
        async with self._client_lock:
            if not self._initialized:
                self._logger.debug("creating new Kubernetes client")
                await asyncio.to_thread(self._init_client, context, kubeconfig)
                if not self._cleanup_registered:
                    atexit.register(self._cleanup_sync)
                    self._cleanup_registered = True
            elif self._context != context:
                raise ValueError(
                    f"KubernetesClientManager already initialized for context "
                    f"'{self._context}'. Cannot connect to context '{context}'. "
                    f"Use separate processes for different clusters."
                )
            self._reference_count += 1
            self._logger.debug(
                "Kubernetes client reference count incremented to %s", self._reference_count
            )
            core_api = self._core_api
        if core_api is None:
            raise RuntimeError("Kubernetes client failed to initialize")
        return core_api

    async def get_dynamic_client(
        self, context: str | None = None, kubeconfig: str | None = None
    ) -> DynamicClient:
        """Get the shared DynamicClient used for OpenKruise CRDs."""
        await self.get_client(context, kubeconfig)
        # Balance the internal get_client() call: the caller releases once.
        async with self._client_lock:
            self._reference_count -= 1
            dynamic_client = self._dynamic_client
        if dynamic_client is None:
            raise RuntimeError("Kubernetes DynamicClient failed to initialize")
        return dynamic_client

    async def release_client(self) -> None:
        """Decrement the reference count; actual cleanup happens at exit."""
        async with self._client_lock:
            if self._reference_count > 0:
                self._reference_count -= 1
                self._logger.debug(
                    "Kubernetes client reference count decremented to %s",
                    self._reference_count,
                )

    def _cleanup_sync(self) -> None:
        """Synchronous cleanup wrapper for atexit."""
        try:
            asyncio.run(self._cleanup())
        except Exception as exc:
            print(f"Error during Kubernetes client cleanup: {exc}", file=sys.stderr)

    async def _cleanup(self) -> None:
        """Close the shared Kubernetes client if it exists."""
        async with self._client_lock:
            if not self._initialized:
                return
            try:
                self._logger.debug("cleaning up Kubernetes client at program exit")
                self._core_api = None
                self._dynamic_client = None
                if self._api_client is not None:
                    self._api_client.close()
                self._api_client = None
                self._initialized = False
            except Exception as exc:
                self._logger.error("error cleaning up Kubernetes client: %s", exc)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge override into base dict for Kubernetes pod specs.

    Dicts merge recursively. The ``containers`` and ``initContainers`` lists
    merge element-wise by index. All other values are replaced.
    """
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        elif (
            key in ("containers", "initContainers")
            and isinstance(result.get(key), list)
            and isinstance(val, list)
        ):
            merged = list(result[key])
            for i, item in enumerate(val):
                if i < len(merged) and isinstance(merged[i], dict) and isinstance(item, dict):
                    merged[i] = _deep_merge(merged[i], item)
                elif i < len(merged):
                    merged[i] = item
                else:
                    merged.append(item)
            result[key] = merged
        else:
            result[key] = val
    return result


def _as_bool(value: Any) -> bool:
    """Coerce kwargs that may arrive as strings from YAML/CLI config."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _as_dict(value: Any) -> dict[str, Any]:
    """Accept a dict, a JSON string, or None."""
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return json.loads(str(value))


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _resource_name(value: str, *, prefix: str = "polar", max_length: int = 63) -> str:
    """Build a DNS-1123 name, hashed so distinct inputs never collide."""
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    digest = hashlib.sha256(value.encode()).hexdigest()[:8]
    room = max(0, max_length - len(prefix) - len(digest) - 2)
    parts = [part for part in (prefix, slug[:room].strip("-"), digest) if part]
    return "-".join(parts)[:max_length].rstrip("-")


def _label_value(value: str) -> str:
    """Sanitize arbitrary text into a valid Kubernetes label value."""
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value)[:63].strip("._-")
    return sanitized or "unknown"


def _string_dict(value: Any) -> dict[str, str]:
    """Coerce a mapping from config into ``dict[str, str]`` for K8s manifests."""
    return {str(key): str(item) for key, item in _as_dict(value).items()}


def _label_dict(value: Any) -> dict[str, str]:
    """Coerce a mapping from config into valid Kubernetes label values."""
    return {str(key): _label_value(str(item)) for key, item in _as_dict(value).items()}


class ACKRuntime(BaseRuntime):
    """One Kubernetes Pod (or claimed OpenKruise sandbox) per rollout session."""

    def __init__(self, spec: RuntimeSpec, session_id: str, session_dir: Path) -> None:
        super().__init__(spec, session_id, session_dir)
        kwargs = spec.kwargs

        self._namespace = str(kwargs.get("namespace") or "").strip()
        if not self._namespace:
            raise ValueError("the ack runtime backend requires runtime.kwargs.namespace")
        self._context = kwargs.get("context")
        self._kubeconfig = kwargs.get("kubeconfig")
        self._image_pull_secret = kwargs.get("image_pull_secret")
        self._service_account = kwargs.get("service_account")
        self._node_selector = _as_dict(kwargs.get("node_selector"))
        self._tolerations = kwargs.get("tolerations") or None
        self._pod_overrides = _as_dict(kwargs.get("pod_overrides"))
        self._memory_limit_multiplier = _as_float(kwargs.get("memory_limit_multiplier"))
        self._pod_ready_timeout = int(kwargs.get("pod_ready_timeout", 300))
        self._user = kwargs.get("user")

        self._use_sandbox_claim = _as_bool(kwargs.get("use_sandbox_claim"))
        self._sandbox_image = kwargs.get("sandbox_image") or None
        self._sandboxset_replicas = int(kwargs.get("sandboxset_replicas", 5))
        self._claim_timeout = int(kwargs.get("claim_timeout", 300))
        self._sandbox_labels = _as_dict(kwargs.get("sandbox_labels"))
        self._sandbox_annotations = _as_dict(kwargs.get("sandbox_annotations"))
        self._sandbox_env_vars = _as_dict(kwargs.get("sandbox_env_vars"))
        self._sandboxset_name = str(
            kwargs.get("sandboxset_name")
            or _resource_name(self._container_image(), prefix="polar-pool")
        )

        self._pod_name = _resource_name(session_id)
        self._claim_name: str | None = None
        self._sandbox_name: str | None = None

        self._client_manager: KubernetesClientManager | None = None
        self._core_api: k8s_client.CoreV1Api | None = None
        # Exec streams need their own ApiClient: the websocket connection must
        # not share state with the request/response client used for CRUD calls.
        self._exec_api: k8s_client.CoreV1Api | None = None
        self._dynamic_client: DynamicClient | None = None
        self._sandboxset_api: Any = None
        self._sandboxclaim_api: Any = None
        self._sandbox_api: Any = None

    @property
    def runtime_id(self) -> str:
        return self._pod_name

    @property
    def supports_cpu_limits(self) -> bool:
        return True

    @property
    def supports_memory_limits(self) -> bool:
        return True

    @property
    def supports_storage_limits(self) -> bool:
        return True

    def resolve_host_path(self, runtime_path: str) -> Path | None:
        """Pods are remote: no runtime path maps back to the host."""
        return None

    def _container_image(self) -> str:
        return str(self._sandbox_image or self.spec.image)

    # ------------------------------------------------------------------
    # Client plumbing
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> None:
        """Ensure the shared Kubernetes client is initialized."""
        if self._client_manager is None:
            self._client_manager = await KubernetesClientManager.get_instance()
        if self._core_api is None:
            self._core_api = await self._client_manager.get_client(
                self._context, kubeconfig=self._kubeconfig
            )
        if self._exec_api is None:
            self._exec_api = k8s_client.CoreV1Api(k8s_client.ApiClient())

        if self._use_sandbox_claim and self._dynamic_client is None:
            self._dynamic_client = await self._client_manager.get_dynamic_client(
                self._context, kubeconfig=self._kubeconfig
            )
            self._sandboxset_api = self._sandbox_api_for("SandboxSet")
            self._sandboxclaim_api = self._sandbox_api_for("SandboxClaim")
            self._sandbox_api = self._sandbox_api_for("Sandbox")

    def _sandbox_api_for(self, kind: str) -> Any:
        if self._dynamic_client is None:
            raise RuntimeError("Kubernetes DynamicClient is not initialized")
        return self._dynamic_client.resources.get(
            api_version=_SANDBOX_API_VERSION, kind=kind
        )

    def _require_core_api(self) -> k8s_client.CoreV1Api:
        if self._core_api is None:
            raise RuntimeError("Kubernetes client is not initialized; call start() first")
        return self._core_api

    def _require_exec_api(self) -> k8s_client.CoreV1Api:
        if self._exec_api is None:
            raise RuntimeError("Kubernetes client is not initialized; call start() first")
        return self._exec_api

    def _require_sandbox_api(self, api: Any, kind: str) -> Any:
        if api is None:
            raise RuntimeError(
                f"{kind} API is not initialized; enable kwargs.use_sandbox_claim"
            )
        return api

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._destroyed:
            raise RuntimeError("ack runtime was already destroyed")
        await self._ensure_client()
        if self._use_sandbox_claim:
            await self._start_with_sandboxclaim()
        else:
            await self._start_with_pod()
        await self._ensure_session_dirs()

    def _resources(self) -> dict[str, Any]:
        """Kubernetes resource requests/limits derived from the RuntimeSpec."""
        requests: dict[str, str] = {}
        if self.spec.cpus is not None:
            requests["cpu"] = str(self.spec.cpus)
        # Use Mi directly to avoid precision loss from integer division.
        if self.spec.memory_mb is not None:
            requests["memory"] = f"{self.spec.memory_mb}Mi"
        if self.spec.storage_mb is not None:
            requests["ephemeral-storage"] = f"{self.spec.storage_mb}Mi"

        resources: dict[str, Any] = {}
        if requests:
            resources["requests"] = requests
        multiplier = self._memory_limit_multiplier
        if multiplier and multiplier > 0 and self.spec.memory_mb is not None:
            limit_mb = int(self.spec.memory_mb * multiplier)
            resources["limits"] = {"memory": f"{limit_mb}Mi"}
        return resources

    def _container_spec(self) -> dict[str, Any]:
        container: dict[str, Any] = {
            "name": "main",
            "image": self._container_image(),
            "command": ["sleep", "infinity"],
            "securityContext": {"privileged": True, "runAsUser": 0},
        }
        resources = self._resources()
        if resources:
            container["resources"] = resources
        return container

    def _scheduling_spec(self) -> dict[str, Any]:
        spec: dict[str, Any] = {}
        if self._image_pull_secret:
            spec["imagePullSecrets"] = [{"name": self._image_pull_secret}]
        if self._service_account:
            spec["serviceAccountName"] = self._service_account
        if self._node_selector:
            spec["nodeSelector"] = self._node_selector
        if self._tolerations:
            spec["tolerations"] = self._tolerations
        return spec

    async def _start_with_pod(self) -> None:
        """Create a long-lived Pod running ``sleep infinity``."""
        core_api = self._require_core_api()
        pod: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": self._pod_name,
                "namespace": self._namespace,
                "labels": {**_POD_LABELS, "session": _label_value(self.session_id)},
            },
            "spec": {
                "restartPolicy": "Never",
                "containers": [self._container_spec()],
                **self._scheduling_spec(),
            },
        }
        if self._pod_overrides:
            pod = _deep_merge(pod, self._pod_overrides)

        try:
            await asyncio.to_thread(
                core_api.create_namespaced_pod, namespace=self._namespace, body=pod
            )
        except ApiException as exc:
            if exc.status != 409:
                raise RuntimeError(f"failed to create pod {self._pod_name}: {exc}") from exc
            # Already exists (e.g. a previous session that never tore down).
            logger.debug("pod %s already exists, recreating", self._pod_name)
            await self._delete_pod()
            await asyncio.to_thread(
                core_api.create_namespaced_pod, namespace=self._namespace, body=pod
            )

        await self._wait_for_pod_ready()

    async def _ensure_session_dirs(self) -> None:
        """Create Polar's well-known session dirs inside the pod."""
        # chmod so harnesses that switch to a non-root user can still write.
        # cwd="/" because this is the bootstrap command: the default working
        # directory (/polar/session) does not exist until this succeeds.
        result = await self.exec(session_dirs_shell_command(chmod=True), cwd="/")
        if result.return_code != 0:
            raise RuntimeError(
                f"failed to create session dirs in pod {self._pod_name}: "
                f"stdout={result.stdout}, stderr={result.stderr}"
            )

    async def _delete_pod(self) -> None:
        """Delete the session Pod and wait for it to disappear."""
        core_api = self._require_core_api()
        try:
            await asyncio.to_thread(
                core_api.delete_namespaced_pod,
                name=self._pod_name,
                namespace=self._namespace,
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=0, propagation_policy="Foreground"
                ),
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise RuntimeError(f"failed to delete pod {self._pod_name}: {exc}") from exc

        for _ in range(60):
            try:
                await asyncio.to_thread(
                    core_api.read_namespaced_pod,
                    name=self._pod_name,
                    namespace=self._namespace,
                )
            except ApiException as exc:
                if exc.status == 404:
                    return
                raise
            await asyncio.sleep(1)
        logger.warning("pod %s did not terminate within 60 seconds", self._pod_name)

    async def stop(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        manager = self._client_manager
        if manager is None:
            return
        try:
            await self._ensure_client()
            if self._use_sandbox_claim and self._claim_name:
                await self._delete_sandboxclaim()
            else:
                await self._delete_pod()
        except Exception as exc:
            logger.warning("failed to tear down ack runtime %s: %s", self._pod_name, exc)
        finally:
            self._client_manager = None
            self._core_api = None
            self._exec_api = None
            self._dynamic_client = None
            try:
                await manager.release_client()
            except Exception as exc:
                logger.error("error releasing Kubernetes client: %s", exc)

    # ------------------------------------------------------------------
    # Exec
    # ------------------------------------------------------------------

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        """Execute a command in the pod over the Kubernetes exec stream."""
        await self._ensure_client()
        exec_api = self._require_exec_api()

        effective_env = {**self.spec.env, **(env or {})}
        full_command = f"bash -lc {shlex.quote(command)}"
        for key, value in effective_env.items():
            full_command = f"{key}={shlex.quote(str(value))} {full_command}"
        effective_cwd = cwd or self.spec.workdir or self.runtime_session_dir
        if effective_cwd:
            full_command = f"cd {shlex.quote(effective_cwd)} && {full_command}"
        if self._user is not None:
            full_command = self._wrap_as_user(full_command)

        logger.debug("executing command in pod %s: %s", self._pod_name, full_command)
        argv = ["sh", "-c", full_command]
        resp = None
        try:
            resp = await asyncio.to_thread(
                stream,
                exec_api.connect_get_namespaced_pod_exec,
                self._pod_name,
                self._namespace,
                command=argv,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
                # Read the raw channel bytes; decoding per websocket frame
                # corrupts multi-byte characters split across frames.
                binary=True,
            )
            reader = asyncio.to_thread(self._read_exec_output, resp)
            if timeout_sec is None:
                stdout, stderr = await reader
            else:
                stdout, stderr = await asyncio.wait_for(reader, timeout_sec)
            # Drain the exit-status channel so returncode is populated.
            resp.run_forever(timeout=0)
            return_code = resp.returncode if resp.returncode is not None else 0
            return ExecResult(stdout=stdout, stderr=stderr, return_code=return_code)
        except TimeoutError:
            return ExecResult(
                stdout=None,
                stderr=f"command timed out after {timeout_sec} seconds",
                return_code=-1,
            )
        except ApiException as exc:
            return ExecResult(stdout=None, stderr=self._api_error_message(exc), return_code=1)
        except Exception as exc:
            return ExecResult(stdout=None, stderr=str(exc), return_code=1)
        finally:
            if resp is not None:
                with suppress(Exception):
                    resp.close()

    def _wrap_as_user(self, command: str) -> str:
        """Wrap a command so it runs as ``kwargs.user`` via ``su``."""
        user = str(self._user)
        # su requires a username; resolve numeric UIDs via getent.
        if user.isdigit():
            user_arg = f"$(getent passwd {user} | cut -d: -f1)"
        else:
            user_arg = shlex.quote(user)
        # Use su (not su -) to preserve the working directory.
        return f"su {user_arg} -s /bin/bash -c {shlex.quote(command)}"

    def _api_error_message(self, exc: ApiException) -> str:
        if exc.status == 404:
            return f"pod {self._pod_name} not found (404)."
        if exc.status == 500:
            body = str(exc.body) if hasattr(exc, "body") else str(exc)
            if "No agent available" in body:
                return f"pod {self._pod_name} unavailable: No agent available."
            return f"internal server error on pod {self._pod_name}: {exc.reason}"
        return f"Kubernetes API error ({exc.status}) on pod {self._pod_name}: {exc.reason}"

    @staticmethod
    def _read_exec_output(resp: Any) -> tuple[str, str]:
        """Read stdout/stderr from a binary exec stream until it closes."""
        stdout = bytearray()
        stderr = bytearray()
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                stdout += resp.read_stdout()
            if resp.peek_stderr():
                stderr += resp.read_stderr()
        return (
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace"),
        )

    async def _wait_for_container_exec_ready(self, max_attempts: int = 60) -> None:
        """Wait until the container accepts exec streams."""
        exec_api = self._require_exec_api()
        last_error: Exception | None = None
        for attempt in range(max_attempts):
            try:
                resp = await asyncio.to_thread(
                    stream,
                    exec_api.connect_get_namespaced_pod_exec,
                    self._pod_name,
                    self._namespace,
                    command=["true"],
                    stderr=False,
                    stdin=False,
                    stdout=True,
                    tty=False,
                    _preload_content=False,
                )
                resp.close()
                return
            except ApiException as exc:
                if "container not found" not in str(exc) and exc.status != 500:
                    raise
                last_error = exc
            except Exception as exc:
                if attempt >= max_attempts - 1:
                    raise
                last_error = exc
            if attempt % 10 == 0:
                logger.debug(
                    "container not ready for exec, attempt %s/%s: %s",
                    attempt + 1,
                    max_attempts,
                    last_error,
                )
            await asyncio.sleep(3)
        raise RuntimeError(f"container not ready for exec after {max_attempts} attempts")

    # ------------------------------------------------------------------
    # File transfer (tar over the exec stream — there is no bind mount)
    # ------------------------------------------------------------------

    async def _mkdir_p(self, remote_path: str) -> None:
        # cwd="/": uploads land in a fresh pod where spec.workdir may not exist
        # yet (e.g. the evaluator's fresh runtime), and a failed `cd` would
        # abort the mkdir before it runs.
        result = await self.exec(f"mkdir -p {shlex.quote(remote_path)}", cwd="/")
        if result.return_code != 0:
            raise RuntimeError(
                f"failed to create {remote_path} in pod {self._pod_name}: {result.stderr}"
            )

    async def _stream_tar_in(self, payload: bytes, target_dir: str) -> None:
        """Extract a tar payload into ``target_dir`` inside the pod."""
        exec_api = self._require_exec_api()
        argv = ["tar", "xf", "-", "-C", target_dir]
        pod_name = self._pod_name

        def _upload_sync() -> None:
            resp = stream(
                exec_api.connect_get_namespaced_pod_exec,
                pod_name,
                self._namespace,
                command=argv,
                stderr=True,
                stdin=True,
                stdout=True,
                tty=False,
                _preload_content=False,
            )
            try:
                resp.write_stdin(payload)
                resp.run_forever(timeout=1)
            except ApiException as exc:
                if exc.status == 500:
                    raise RuntimeError(f"pod {pod_name} returned 500 error during upload") from exc
                raise
            except Exception as exc:
                raise RuntimeError(f"failed to write tar data to pod {pod_name}: {exc}") from exc
            finally:
                with suppress(Exception):
                    resp.close()

        await asyncio.to_thread(_upload_sync)

    async def _stream_tar_out(self, argv: list[str]) -> tuple[bytes, str]:
        """Run a tar-producing command in the pod and capture raw bytes.

        ``binary=True`` keeps the websocket payload as bytes: the default text
        mode decodes each frame as UTF-8 with replacement characters, which
        silently corrupts binary payloads such as tar streams.
        """
        exec_api = self._require_exec_api()
        pod_name = self._pod_name

        def _download_sync() -> tuple[bytes, str]:
            resp = stream(
                exec_api.connect_get_namespaced_pod_exec,
                pod_name,
                self._namespace,
                command=argv,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
                binary=True,
            )
            stdout_data = bytearray()
            stderr_data = bytearray()
            try:
                while resp.is_open():
                    resp.update(timeout=1)
                    if resp.peek_stdout():
                        stdout_data += resp.read_stdout()
                    if resp.peek_stderr():
                        stderr_data += resp.read_stderr()
            finally:
                with suppress(Exception):
                    resp.close()
            return bytes(stdout_data), stderr_data.decode("utf-8", errors="replace")

        try:
            return await asyncio.to_thread(_download_sync)
        except ApiException as exc:
            if exc.status == 404:
                raise RuntimeError(f"pod {pod_name} not found (404).") from exc
            if exc.status == 500:
                raise RuntimeError(f"pod {pod_name} is in an error state (500).") from exc
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def upload_file(self, local_path: str, remote_path: str) -> None:
        await self._ensure_client()
        source = Path(local_path)
        if not source.is_file():
            raise FileNotFoundError(f"source path does not exist: {local_path}")
        await self._wait_for_container_exec_ready()

        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as tar:
            tar.add(str(source), arcname=PurePosixPath(remote_path).name)

        target_dir = str(PurePosixPath(remote_path).parent)
        await self._mkdir_p(target_dir)
        await self._stream_tar_in(buffer.getvalue(), target_dir)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def upload_dir(self, local_path: str, remote_path: str) -> None:
        await self._ensure_client()
        source = Path(local_path)
        if not source.is_dir():
            raise NotADirectoryError(f"source directory does not exist: {local_path}")
        await self._wait_for_container_exec_ready()

        buffer = io.BytesIO()
        file_count = 0
        with tarfile.open(fileobj=buffer, mode="w") as tar:
            for item in source.rglob("*"):
                if item.is_file():
                    tar.add(str(item), arcname=str(item.relative_to(source)))
                    file_count += 1

        await self._mkdir_p(remote_path)
        if file_count == 0:
            logger.debug("no files to upload from %s", local_path)
            return

        payload = buffer.getvalue()
        await self._stream_tar_in(payload, remote_path)
        logger.debug(
            "uploaded %s files (%s bytes) to %s in pod %s",
            file_count,
            len(payload),
            remote_path,
            self._pod_name,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def download_file(self, remote_path: str, local_path: str) -> None:
        await self._ensure_client()
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        tar_data, stderr_data = await self._stream_tar_out(["tar", "cf", "-", remote_path])
        if not tar_data:
            raise RuntimeError(
                f"no data received when downloading {remote_path} from pod "
                f"{self._pod_name}: {stderr_data.strip()}"
            )

        stripped = remote_path.lstrip("/")
        with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r") as tar:
            for member in tar.getmembers():
                if member.name in (remote_path, stripped):
                    member.name = target.name
                    tar.extract(member, path=str(target.parent), filter="data")
                    return
        raise RuntimeError(
            f"{remote_path} was not present in the tar stream from pod {self._pod_name}"
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def download_dir(self, remote_path: str, local_path: str) -> None:
        await self._ensure_client()
        target = Path(local_path)
        target.mkdir(parents=True, exist_ok=True)

        quoted = shlex.quote(remote_path)
        tar_data, stderr_data = await self._stream_tar_out(
            ["sh", "-c", f"cd {quoted} && tar cf - ."]
        )
        if stderr_data and (
            "No such file or directory" in stderr_data or "cannot cd" in stderr_data
        ):
            raise RuntimeError(
                f"failed to access directory {remote_path} in pod {self._pod_name}: "
                f"{stderr_data.strip()}"
            )
        if not tar_data:
            raise RuntimeError(
                f"no data received when downloading {remote_path} from pod {self._pod_name}."
            )

        try:
            with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r") as tar:
                tar.extractall(path=str(target), filter="data")
        except tarfile.TarError as exc:
            raise RuntimeError(
                f"failed to extract directory {remote_path} from pod {self._pod_name}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Pod readiness
    # ------------------------------------------------------------------

    async def _wait_for_pod_ready(self) -> None:
        """Poll the pod until every container is ready."""
        core_api = self._require_core_api()
        timeout_sec = self._pod_ready_timeout
        logger.debug("waiting for pod %s to be ready", self._pod_name)

        for attempt in range(timeout_sec):
            try:
                pod = await asyncio.to_thread(
                    core_api.read_namespaced_pod,
                    name=self._pod_name,
                    namespace=self._namespace,
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise RuntimeError(
                        f"Kubernetes API error: {exc.status} - {exc.reason}"
                    ) from exc
                pod = None

            if pod is not None:
                phase = pod.status.phase
                if phase == "Running" and pod.status.container_statuses:
                    if all(container.ready for container in pod.status.container_statuses):
                        logger.debug("pod %s is ready", self._pod_name)
                        return
                elif phase in ("Failed", "Unknown", "Error"):
                    raise RuntimeError(f"pod failed to start: {self._pod_failure_summary(pod)}")
                elif phase == "Pending":
                    self._raise_for_image_pull_failure(pod)
                if attempt % 10 == 0:
                    logger.debug("pod %s status: %s (%ss elapsed)", self._pod_name, phase, attempt)

            await asyncio.sleep(1)

        raise RuntimeError(f"pod {self._pod_name} not ready after {timeout_sec} seconds")

    @staticmethod
    def _raise_for_image_pull_failure(pod: Any) -> None:
        for container in pod.status.container_statuses or []:
            waiting = container.state.waiting if container.state else None
            if not waiting or not waiting.reason:
                continue
            if "ImagePullBackOff" in waiting.reason or "ErrImagePull" in waiting.reason:
                raise RuntimeError(
                    f"failed to pull image: {waiting.message or waiting.reason}"
                )

    @staticmethod
    def _pod_failure_summary(pod: Any) -> str:
        """Summarize why a pod failed to start."""
        reasons: list[str] = []
        if pod.status.reason:
            reasons.append(f"Reason: {pod.status.reason}")
        if pod.status.message:
            reasons.append(f"Message: {pod.status.message}")
        for container in pod.status.container_statuses or []:
            state = container.state
            if state is None:
                continue
            if state.waiting:
                reasons.append(f"Container {container.name} waiting: {state.waiting.reason}")
            elif state.terminated:
                reasons.append(
                    f"Container {container.name} terminated: {state.terminated.reason} "
                    f"(exit code {state.terminated.exit_code})"
                )
        return "; ".join(reasons) if reasons else "Unknown error"

    # ------------------------------------------------------------------
    # OpenKruise SandboxSet / SandboxClaim (pre-warmed pool)
    # ------------------------------------------------------------------

    async def _ensure_sandboxset(self) -> None:
        """Ensure the warm SandboxSet pool exists, creating it if necessary.

        The pool template mirrors the Pod spec used in standard mode. It is
        shared across sessions and deliberately left in place on stop().
        """
        api = self._require_sandbox_api(self._sandboxset_api, "SandboxSet")
        name = self._sandboxset_name
        image = self._container_image()

        try:
            existing = await asyncio.to_thread(api.get, name=name, namespace=self._namespace)
            logger.debug(
                "SandboxSet %s already exists (replicas=%s)",
                name,
                getattr(existing.spec, "replicas", "?"),
            )
            return
        except ApiException as exc:
            if exc.status != 404:
                raise RuntimeError(f"failed to check SandboxSet {name}: {exc}") from exc

        template_spec: dict[str, Any] = {
            "containers": [self._container_spec()],
            **self._scheduling_spec(),
        }
        override_spec = self._pod_overrides.get("spec", {})
        if override_spec:
            template_spec = _deep_merge(template_spec, override_spec)
        override_meta = self._pod_overrides.get("metadata", {})
        annotations = _string_dict(self._sandbox_annotations)
        pool_labels = {
            **_POD_LABELS,
            "image": _label_value(image),
            **_label_dict(self._sandbox_labels),
        }
        metadata: dict[str, Any] = {
            "name": name,
            "namespace": self._namespace,
            "labels": pool_labels,
        }
        if annotations:
            metadata["annotations"] = annotations
        template_metadata: dict[str, Any] = {
            "labels": {**pool_labels, **override_meta.get("labels", {})},
            "annotations": {**annotations, **override_meta.get("annotations", {})},
        }

        body = {
            "apiVersion": _SANDBOX_API_VERSION,
            "kind": "SandboxSet",
            "metadata": metadata,
            "spec": {
                "replicas": self._sandboxset_replicas,
                "template": {"metadata": template_metadata, "spec": template_spec},
            },
        }

        try:
            await asyncio.to_thread(api.create, body=body, namespace=self._namespace)
            logger.debug(
                "created SandboxSet %s with %s replicas using image %s",
                name,
                self._sandboxset_replicas,
                image,
            )
        except ApiException as exc:
            if exc.status == 409:
                # Race: another session created the pool between check and create.
                logger.debug("SandboxSet %s was created concurrently", name)
            else:
                raise RuntimeError(f"failed to create SandboxSet {name}: {exc}") from exc

    async def _start_with_sandboxclaim(self) -> None:
        """Claim a pre-warmed sandbox and wait for its pod to be ready."""
        api = self._require_sandbox_api(self._sandboxclaim_api, "SandboxClaim")
        await self._ensure_sandboxset()

        claim_name = _resource_name(f"{self.session_id}-claim", prefix="polar")
        self._claim_name = claim_name
        logger.debug(
            "creating SandboxClaim %s from SandboxSet %s", claim_name, self._sandboxset_name
        )

        annotations = _string_dict(self._sandbox_annotations)
        metadata: dict[str, Any] = {
            "name": claim_name,
            "namespace": self._namespace,
            "labels": {
                **_POD_LABELS,
                "session": _label_value(self.session_id),
                **_label_dict(self._sandbox_labels),
            },
        }
        if annotations:
            metadata["annotations"] = annotations
        spec: dict[str, Any] = {
            "templateName": self._sandboxset_name,
            "replicas": 1,
            "claimTimeout": f"{self._claim_timeout}s",
            "createOnNoStock": True,
        }
        if self._sandbox_env_vars:
            spec["envVars"] = _string_dict(self._sandbox_env_vars)
        body = {
            "apiVersion": _SANDBOX_API_VERSION,
            "kind": "SandboxClaim",
            "metadata": metadata,
            "spec": spec,
        }

        try:
            await asyncio.to_thread(api.create, body=body, namespace=self._namespace)
            logger.debug("SandboxClaim %s created", claim_name)
        except ApiException as exc:
            if exc.status == 409:
                logger.debug("SandboxClaim %s already exists, recreating", claim_name)
                await self._delete_sandboxclaim()
                self._claim_name = claim_name
                await asyncio.to_thread(api.create, body=body, namespace=self._namespace)
            elif exc.status == 403:
                raise RuntimeError(
                    "permission denied. Ensure the ServiceAccount has RBAC for the "
                    "agents.kruise.io API group (sandboxclaims, sandboxes)."
                ) from exc
            else:
                raise RuntimeError(f"failed to create SandboxClaim {claim_name}: {exc}") from exc

        await self._wait_for_claim_completed()
        await self._get_claimed_sandbox()
        await self._wait_for_pod_ready()

    async def _wait_for_claim_completed(self) -> None:
        """Wait for the SandboxClaim to reach the Completed phase."""
        api = self._require_sandbox_api(self._sandboxclaim_api, "SandboxClaim")
        claim_name = self._claim_name
        if claim_name is None:
            raise RuntimeError("no SandboxClaim to wait for")
        # Extra buffer on top of the claim's own timeout.
        timeout_sec = self._claim_timeout + 60
        logger.debug("waiting for SandboxClaim %s to complete", claim_name)

        for elapsed in range(0, timeout_sec, 2):
            try:
                claim = await asyncio.to_thread(
                    api.get, name=claim_name, namespace=self._namespace
                )
                phase = getattr(claim.status, "phase", None)
                if phase == "Completed":
                    claimed = getattr(claim.status, "claimedReplicas", 0)
                    if claimed >= 1:
                        logger.debug("SandboxClaim %s completed successfully", claim_name)
                        return
                    message = getattr(claim.status, "message", "Unknown")
                    raise RuntimeError(f"SandboxClaim Completed but claimedReplicas=0: {message}")
                if phase == "Failed":
                    message = getattr(claim.status, "message", "Unknown error")
                    raise RuntimeError(f"SandboxClaim failed: {message}")
                if phase == "Claiming" and elapsed % 30 == 0:
                    logger.debug("claiming... (%ss/%ss)", elapsed, timeout_sec)
            except ApiException as exc:
                if exc.status != 404:
                    raise RuntimeError(
                        f"Kubernetes API error ({exc.status}): {exc.reason}"
                    ) from exc
            await asyncio.sleep(2)

        raise RuntimeError(
            f"SandboxClaim {claim_name} not completed after {timeout_sec}s; check the "
            f"{self._sandboxset_name} pool pods (image pull, scheduling, quota)"
        )

    async def _get_claimed_sandbox(self) -> None:
        """Resolve the Sandbox (and its pod name) claimed by this session.

        Selection must be exact: falling back to an arbitrary pool member would
        run the session in a sandbox the claim does not own, then orphan the
        real one at teardown (the controller does not release a claimed sandbox
        when its claim is deleted).
        """
        api = self._require_sandbox_api(self._sandbox_api, "Sandbox")
        claim_name = self._claim_name
        try:
            sandboxes = await asyncio.to_thread(
                api.get,
                namespace=self._namespace,
                label_selector=f"{_CLAIM_NAME_LABEL}={claim_name}",
            )
            matches = list(getattr(sandboxes, "items", None) or [])
            if not matches:
                # Controllers that do not index the label: scan and match
                # explicitly. An unmatched scan must yield nothing, never the
                # whole list.
                listed = await asyncio.to_thread(api.get, namespace=self._namespace)
                matches = [
                    sandbox
                    for sandbox in (getattr(listed, "items", None) or [])
                    if self._is_claimed_by(sandbox, claim_name)
                ]
            if not matches:
                raise RuntimeError(f"no Sandbox is bound to claim {claim_name}")
            if len(matches) > 1:
                logger.debug(
                    "claim %s bound %d sandboxes, using the first", claim_name, len(matches)
                )
            sandbox = sorted(matches, key=lambda item: item.metadata.name)[0]
            self._sandbox_name = sandbox.metadata.name
            self._pod_name = self._sandbox_name
            logger.debug("got Sandbox %s -> Pod %s", self._sandbox_name, self._pod_name)
        except ApiException as exc:
            raise RuntimeError(f"failed to get Sandbox: {exc}") from exc

    @staticmethod
    def _is_claimed_by(sandbox: Any, claim_name: str) -> bool:
        """True when *sandbox* carries the binding label/annotation for the claim."""
        metadata = getattr(sandbox, "metadata", None)
        for mapping in (
            getattr(metadata, "labels", None),
            getattr(metadata, "annotations", None),
        ):
            if isinstance(mapping, dict) and mapping.get(_CLAIM_NAME_LABEL) == claim_name:
                return True
        return False

    async def _delete_sandboxclaim(self) -> None:
        """Delete the SandboxClaim and the sandbox it claimed."""
        if not self._claim_name:
            return
        api = self._require_sandbox_api(self._sandboxclaim_api, "SandboxClaim")
        claim_name = self._claim_name

        # Delete the Sandbox first so it does not return to the warm pool.
        if self._sandbox_name:
            await self._delete_sandbox()

        try:
            await asyncio.to_thread(
                api.delete,
                name=claim_name,
                namespace=self._namespace,
                body={"propagationPolicy": "Foreground"},
            )
            logger.debug("SandboxClaim %s deleted", claim_name)
        except ApiException as exc:
            if exc.status != 404:
                logger.warning("failed to delete SandboxClaim %s: %s", claim_name, exc)
            return

        for _ in range(60):
            try:
                await asyncio.to_thread(api.get, name=claim_name, namespace=self._namespace)
            except ApiException as exc:
                if exc.status == 404:
                    return
                raise
            await asyncio.sleep(1)
        logger.warning("SandboxClaim %s not deleted within timeout", claim_name)

    async def _delete_sandbox(self) -> None:
        """Delete the claimed Sandbox."""
        if not self._sandbox_name:
            return
        api = self._require_sandbox_api(self._sandbox_api, "Sandbox")
        sandbox_name = self._sandbox_name

        try:
            await asyncio.to_thread(api.delete, name=sandbox_name, namespace=self._namespace)
            logger.debug("Sandbox %s deleted", sandbox_name)
        except ApiException as exc:
            if exc.status != 404:
                logger.warning("failed to delete Sandbox %s: %s", sandbox_name, exc)
            return

        for _ in range(60):
            try:
                await asyncio.to_thread(api.get, name=sandbox_name, namespace=self._namespace)
            except ApiException as exc:
                if exc.status == 404:
                    return
                raise
            await asyncio.sleep(1)
        logger.warning("Sandbox %s not deleted within timeout", sandbox_name)
