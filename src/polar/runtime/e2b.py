"""E2B-backed rollout runtime.

Migrated from Harbor's ``E2BEnvironment`` onto Polar's runtime contract: one
cloud sandbox per session, shared by the init → run → eval stages.

There is no host bind mount here, so uploads/downloads go through the E2B
filesystem API and the well-known ``/polar/session`` directories are created
inside the sandbox on start.

Config (``RuntimeSpec``)
------------------------
- ``image`` — image the sandbox template is built from.
- ``env`` — injected at sandbox creation and merged into every ``exec``.
- ``cpus`` / ``memory_mb`` — baked into the template when it is built.
- ``allow_internet=False`` — create the sandbox without internet access.
- ``kwargs.template`` *(str)* — reuse a pre-built template alias.
- ``kwargs.build_template`` *(bool)* — build the alias when it is missing.
  Defaults to True for a derived alias, False for an explicit ``template``.
- ``kwargs.allow_out`` *(list[str])* — egress allowlist; all else is denied.
- ``kwargs.user`` *(str, default ``root``)* — user that commands run as.
- ``kwargs.sandbox_timeout`` *(int, default 86400)* — sandbox lifetime, seconds.
- ``kwargs.metadata`` *(dict)* — extra metadata attached to the sandbox.

Requires the ``e2b`` extra (``uv pip install 'polar[e2b]'``) and ``E2B_API_KEY``.

Self-hosted control planes
--------------------------
Every request here goes through the official ``e2b`` SDK, so the endpoints come
from the SDK's own environment: ``E2B_API_KEY``, ``E2B_DOMAIN``, ``E2B_API_URL``
(control-plane override) and ``E2B_SANDBOX_URL`` (data-plane override). A plane
that implements neither template builds nor alias lookup — the common case for a
self-hosted manager — must be driven with ``kwargs.template`` naming a template
that already exists there, otherwise ``start()`` tries to build one and fails.
The sandbox pod also needs ``CAP_SYS_RESOURCE``: envd prefixes each command with
an ``oom_score_adj`` write, and without the capability that write fails, so the
command never runs (exit 1, ``echo: I/O error``) even though the filesystem API
still works. See ``RUNBOOK.md`` → *E2B on a self-hosted control plane*.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shlex
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Any

from polar.runtime.base import BaseRuntime, session_dirs_shell_command
from polar.runtime.models import ExecResult, RuntimeSpec

logger = logging.getLogger(__name__)

try:
    import httpcore
    from e2b import (
        ALL_TRAFFIC,
        AsyncSandbox,
        AsyncTemplate,
        FileType,
        SandboxNetworkOpts,
        Template,
    )
    from e2b.exceptions import RateLimitException
    from e2b.sandbox.commands.command_handle import CommandExitException
    from e2b.sandbox.filesystem.filesystem import WriteEntry
    from e2b.sandbox_async.commands.command_handle import AsyncCommandHandle
    from tenacity import (
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
        wait_random_exponential,
    )

    # Retry only failures that prove the command never reached the daemon, so
    # replay cannot duplicate side effects: connection-establishment errors (no
    # request bytes sent yet) and 429s (rejected before envd spawns anything).
    # Use the leaf classes -- ConnectTimeout/PoolTimeout share a base with the
    # post-dispatch ReadTimeout, which must NOT be retried.
    _DISPATCH_RETRYABLE: tuple[type[BaseException], ...] = (
        httpcore.ConnectError,
        httpcore.ConnectTimeout,
        httpcore.PoolTimeout,
        RateLimitException,
    )
except ImportError as exc:
    raise RuntimeError(
        "the 'e2b' runtime backend requires the e2b extra: uv pip install 'polar[e2b]'"
    ) from exc


class E2BRuntime(BaseRuntime):
    """Long-lived E2B sandbox used across init, run, and post-run."""

    _UPLOAD_BATCH_SIZE = 20
    _SANDBOX_TIMEOUT_SEC = 86_400
    # ``exec`` enforces the timeout locally so Polar sees its ``-1`` convention;
    # the daemon-side timeout is only a safety net for a dropped connection.
    _DAEMON_TIMEOUT_GRACE_SEC = 10

    def __init__(self, spec: RuntimeSpec, session_id: str, session_dir: Path) -> None:
        super().__init__(spec, session_id, session_dir)
        if not os.environ.get("E2B_API_KEY"):
            raise RuntimeError(
                "the e2b runtime backend requires E2B_API_KEY to be set"
            )
        self._sandbox: AsyncSandbox | None = None
        self._template_name, self._build_template = self._resolve_template()
        self._user = str(spec.kwargs.get("user", "root"))

    def _resolve_template(self) -> tuple[str, bool]:
        """Return ``(template alias, build it when missing)``.

        ``kwargs.template`` reuses a pre-built alias; otherwise the alias is
        derived from ``spec.image`` and hashed, so a new image builds a new
        template while concurrent sessions share one.
        """
        explicit = self.spec.kwargs.get("template")
        if explicit:
            alias = str(explicit).strip()
            if not alias:
                raise ValueError("runtime.kwargs.template must be non-empty")
            return alias, bool(self.spec.kwargs.get("build_template", False))
        digest = hashlib.sha256(self.spec.image.encode()).hexdigest()[:12]
        slug = re.sub(r"[^a-z0-9]+", "-", self.spec.image.lower()).strip("-")[:40]
        alias = f"polar-{slug}-{digest}" if slug else f"polar-{digest}"
        return alias, bool(self.spec.kwargs.get("build_template", True))

    @property
    def runtime_id(self) -> str:
        if self._sandbox is None:
            return self._template_name
        return self._sandbox.sandbox_id

    @property
    def can_disable_internet(self) -> bool:
        return True

    @property
    def supports_cpu_limits(self) -> bool:
        return True

    @property
    def supports_memory_limits(self) -> bool:
        return True

    def resolve_host_path(self, runtime_path: str) -> Path | None:
        """E2B sandboxes are remote: no runtime path maps back to the host."""
        return None

    def _require_sandbox(self) -> AsyncSandbox:
        if self._sandbox is None:
            raise RuntimeError("e2b sandbox not started; call start() first")
        return self._sandbox

    def _sandbox_network_opts(self) -> SandboxNetworkOpts | None:
        """Egress allowlist applied at sandbox creation, when configured."""
        if not self.spec.allow_internet:
            return None
        allow_out = self.spec.kwargs.get("allow_out")
        if not allow_out:
            return None
        return {
            "allow_out": [str(host) for host in allow_out],
            "deny_out": [ALL_TRAFFIC],
        }

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _template_exists(self) -> bool:
        return await AsyncTemplate.alias_exists(self._template_name)

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _create_template(self) -> None:
        build_kwargs: dict[str, Any] = {
            "template": Template().from_image(image=self.spec.image),
            "alias": self._template_name,
        }
        if self.spec.cpus is not None:
            build_kwargs["cpu_count"] = self.spec.cpus
        if self.spec.memory_mb is not None:
            build_kwargs["memory_mb"] = self.spec.memory_mb
        await AsyncTemplate.build(**build_kwargs)

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _create_sandbox(self) -> None:
        extra_metadata = self.spec.kwargs.get("metadata") or {}
        metadata = {
            "backend": "e2b",
            "session_id": self.session_id,
            "image": self.spec.image,
            **{str(key): str(value) for key, value in extra_metadata.items()},
        }
        self._sandbox = await AsyncSandbox.create(
            template=self._template_name,
            metadata=metadata,
            envs=dict(self.spec.env),
            timeout=int(self.spec.kwargs.get("sandbox_timeout", self._SANDBOX_TIMEOUT_SEC)),
            allow_internet_access=self.spec.allow_internet,
            network=self._sandbox_network_opts(),
        )

    async def start(self) -> None:
        if self._destroyed:
            raise RuntimeError("e2b runtime was already destroyed")
        if self._build_template and not await self._template_exists():
            logger.debug(
                "building e2b template %s from image %s",
                self._template_name,
                self.spec.image,
            )
            await self._create_template()
        await self._create_sandbox()
        if self._sandbox is None:
            raise RuntimeError("e2b sandbox was not created")
        logger.debug("started e2b sandbox %s", self._sandbox.sandbox_id)
        await self._ensure_session_dirs()

    async def _ensure_session_dirs(self) -> None:
        """Create Polar's well-known session dirs inside the sandbox."""
        # cwd="/" because this is the bootstrap command: the default working
        # directory (/polar/session) does not exist until this succeeds.
        result = await self.exec(session_dirs_shell_command(chmod=True), cwd="/")
        if result.return_code != 0:
            raise RuntimeError(
                f"failed to create session dirs in sandbox {self.runtime_id}: "
                f"{result.stderr or result.stdout}"
            )

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _kill_sandbox(self, sandbox: AsyncSandbox) -> None:
        await sandbox.kill()

    async def stop(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        sandbox = self._sandbox
        self._sandbox = None
        if sandbox is None:
            logger.debug("e2b sandbox for %s was already removed", self.session_id)
            return
        try:
            await self._kill_sandbox(sandbox)
        except Exception as exc:
            logger.warning(
                "failed to kill e2b sandbox %s for session %s: %s",
                sandbox.sandbox_id,
                self.session_id,
                exc,
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(_DISPATCH_RETRYABLE),
        reraise=True,
    )
    async def _dispatch_command(
        self,
        command: str,
        *,
        cwd: str,
        env: dict[str, str],
        timeout_sec: float | None,
        user: str,
    ) -> AsyncCommandHandle:
        """Start ``command`` in the background and return its handle.

        Retries only ``_DISPATCH_RETRYABLE`` failures; once a pid exists the
        command is running, so re-dispatch would duplicate side effects.
        """
        sandbox = self._require_sandbox()
        daemon_timeout = (
            0 if timeout_sec is None else int(timeout_sec) + self._DAEMON_TIMEOUT_GRACE_SEC
        )
        return await sandbox.commands.run(
            cmd=command,
            background=True,
            cwd=cwd,
            envs=env,
            timeout=daemon_timeout,
            user=user,
        )

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        handle = await self._dispatch_command(
            command,
            cwd=cwd or self.spec.workdir or self.runtime_session_dir,
            env={**self.spec.env, **(env or {})},
            timeout_sec=timeout_sec,
            user=self._user,
        )

        # Deliberately not retried: the command is already running on the
        # daemon, so a transport failure here must propagate rather than
        # re-dispatch and double-execute. A non-zero exit is a real result.
        try:
            if timeout_sec is None:
                result = await handle.wait()
            else:
                result = await asyncio.wait_for(handle.wait(), timeout_sec)
        except CommandExitException as exc:
            result = exc
        except TimeoutError:
            with suppress(Exception):
                await handle.kill()
            return ExecResult(
                stdout=handle.stdout or None,
                stderr=f"command timed out after {timeout_sec} seconds",
                return_code=-1,
            )

        return ExecResult(
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.exit_code,
        )

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def upload_file(self, local_path: str, remote_path: str) -> None:
        sandbox = self._require_sandbox()
        source = Path(local_path)
        if not source.is_file():
            raise FileNotFoundError(f"source path does not exist: {local_path}")
        await sandbox.files.write(remote_path, source.read_bytes())

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def upload_dir(self, local_path: str, remote_path: str) -> None:
        sandbox = self._require_sandbox()
        source = Path(local_path)
        if not source.is_dir():
            raise NotADirectoryError(f"source directory does not exist: {local_path}")

        target_root = PurePosixPath(remote_path)
        entries: list[WriteEntry] = []
        for file_path in source.rglob("*"):
            if file_path.is_file():
                entries.append(
                    WriteEntry(
                        path=str(target_root / file_path.relative_to(source).as_posix()),
                        data=file_path.read_bytes(),
                    )
                )

        if not entries:
            await self.exec(f"mkdir -p {shlex.quote(remote_path)}")
            return

        for start in range(0, len(entries), self._UPLOAD_BATCH_SIZE):
            await sandbox.files.write_files(entries[start : start + self._UPLOAD_BATCH_SIZE])

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def download_file(self, remote_path: str, local_path: str) -> None:
        sandbox = self._require_sandbox()
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(await sandbox.files.read(remote_path, format="bytes"))

    async def download_dir(self, remote_path: str, local_path: str) -> None:
        await self._download_dir(remote_path, Path(local_path))

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _download_dir(self, remote_dir: str, local_dir: Path) -> None:
        """Recursively copy a sandbox directory to the host.

        Walked entry-by-entry (rather than streamed as a tar) because sandbox
        command output is decoded as text and would corrupt binary files.
        """
        sandbox = self._require_sandbox()
        root = PurePosixPath(remote_dir.rstrip("/") or "/")
        local_dir.mkdir(parents=True, exist_ok=True)

        for entry in await sandbox.files.list(remote_dir):
            entry_path = PurePosixPath(entry.path)
            try:
                relative = entry_path.relative_to(root)
            except ValueError:
                relative = PurePosixPath(entry_path.name)
            if entry.type == FileType.DIR:
                await self._download_dir(entry.path, local_dir / relative)
            elif entry.type == FileType.FILE:
                await self.download_file(entry.path, str(local_dir / relative))
