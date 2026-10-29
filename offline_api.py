"""In-memory Kubernetes-shaped API for deterministic reconciliation checks.

This is a test harness, not evidence of a live Kubernetes deployment.
"""
from __future__ import annotations

from copy import deepcopy


class MemoryAPI:
    def __init__(self):
        self.objects: dict[tuple[str, str, str], dict] = {}
        self.creates = 0
        self.patches = 0

    @staticmethod
    def key(kind: str, namespace: str, name: str) -> tuple[str, str, str]:
        return kind, namespace, name

    def get(self, kind: str, namespace: str, name: str) -> dict | None:
        value = self.objects.get(self.key(kind, namespace, name))
        return deepcopy(value) if value is not None else None

    def create(self, kind: str, namespace: str, body: dict) -> None:
        key = self.key(kind, namespace, body["metadata"]["name"])
        if key in self.objects:
            raise ValueError("duplicate object")
        self.objects[key] = deepcopy(body)
        self.creates += 1

    def patch(self, kind: str, namespace: str, name: str, body: dict) -> None:
        key = self.key(kind, namespace, name)
        if key not in self.objects:
            raise KeyError(key)
        _merge(self.objects[key], body)
        self.patches += 1

    def delete(self, kind: str, namespace: str, name: str) -> None:
        self.objects.pop(self.key(kind, namespace, name), None)


def _merge(target: dict, patch: dict) -> None:
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge(target[key], value)
        else:
            target[key] = deepcopy(value)
