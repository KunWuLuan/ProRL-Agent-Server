"""Process-wide Kubernetes client sharing for the ACK backend.

A gateway node drives many concurrent sessions against one cluster, so the
client is created once, reference-counted, and closed at interpreter exit rather
than per session.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import sys
from typing import Any

from polar.runtime.ack._sdk import DynamicClient, k8s_client, k8s_config

logger = logging.getLogger(__name__)


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
