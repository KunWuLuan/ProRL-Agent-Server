"""Manifest-building and config-coercion helpers for the ACK backend.

These are pure functions shared by both allocation modes: deep-merging
``kwargs.pod_overrides`` into a manifest, coercing kwargs that arrive as strings
from YAML/CLI config, and deriving DNS-1123 resource names and label values.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


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
