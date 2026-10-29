"""Declarative reconciliation for a small InferenceService custom resource.

The core accepts an adapter so the same decisions run against Kubernetes and the
deterministic local test API.  It never claims to serve an actual ML model.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


GROUP = "portfolio.nikhil.dev"
VERSION = "v1alpha1"
PLURAL = "inferenceservices"


@dataclass(frozen=True)
class Desired:
    namespace: str
    name: str
    uid: str
    image: str
    replicas: int
    port: int

    @classmethod
    def from_resource(cls, resource: dict) -> "Desired":
        meta = resource["metadata"]
        spec = resource["spec"]
        image = spec["image"]
        replicas = spec.get("replicas", 1)
        port = spec.get("port", 8080)
        if not isinstance(image, str) or not image.strip():
            raise ValueError("spec.image must be a nonempty string")
        if type(replicas) is not int or not 0 <= replicas <= 10:
            raise ValueError("spec.replicas must be an integer from 0 to 10")
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("spec.port must be an integer from 1 to 65535")
        return cls(meta["namespace"], meta["name"], meta["uid"], image, replicas, port)


class ResourceAPI(Protocol):
    def get(self, kind: str, namespace: str, name: str) -> dict | None: ...
    def create(self, kind: str, namespace: str, body: dict) -> None: ...
    def patch(self, kind: str, namespace: str, name: str, body: dict) -> None: ...


def _metadata(d: Desired) -> dict:
    return {
        "name": d.name,
        "namespace": d.namespace,
        "labels": {"app.kubernetes.io/name": d.name, "app.kubernetes.io/managed-by": "inference-recovery-operator"},
        "ownerReferences": [{
            "apiVersion": f"{GROUP}/{VERSION}", "kind": "InferenceService",
            "name": d.name, "uid": d.uid, "controller": True,
            "blockOwnerDeletion": True,
        }],
    }


def deployment(d: Desired) -> dict:
    selector = {"app.kubernetes.io/name": d.name}
    return {
        "apiVersion": "apps/v1", "kind": "Deployment", "metadata": _metadata(d),
        "spec": {
            "replicas": d.replicas,
            "selector": {"matchLabels": selector},
            "template": {
                "metadata": {"labels": selector},
                "spec": {"containers": [{
                    "name": "model-server", "image": d.image,
                    "ports": [{"containerPort": d.port}],
                }]},
            },
        },
    }


def service(d: Desired) -> dict:
    return {
        "apiVersion": "v1", "kind": "Service", "metadata": _metadata(d),
        "spec": {
            "type": "ClusterIP", "selector": {"app.kubernetes.io/name": d.name},
            "ports": [{"name": "http", "port": d.port, "targetPort": d.port, "protocol": "TCP"}],
        },
    }


def _owned_by(current: dict, d: Desired) -> bool:
    return any(ref.get("uid") == d.uid and ref.get("kind") == "InferenceService"
               for ref in current.get("metadata", {}).get("ownerReferences", []))


def _drifted(kind: str, current: dict, wanted: dict) -> bool:
    spec = current.get("spec", {})
    target = wanted["spec"]
    if kind == "Deployment":
        containers = spec.get("template", {}).get("spec", {}).get("containers", [])
        target_container = target["template"]["spec"]["containers"][0]
        if len(containers) != 1:
            return True
        return (spec.get("replicas") != target["replicas"]
                or containers[0].get("image") != target_container["image"]
                or containers[0].get("ports", [{}])[0].get("containerPort") != target_container["ports"][0]["containerPort"])
    ports = spec.get("ports", [])
    if len(ports) != 1:
        return True
    return (spec.get("selector") != target["selector"]
            or ports[0].get("port") != target["ports"][0]["port"]
            or ports[0].get("targetPort") != target["ports"][0]["targetPort"])


def reconcile(api: ResourceAPI, resource: dict) -> list[str]:
    """Create missing children and patch drift. Deterministic names prevent duplicates."""
    d = Desired.from_resource(resource)
    actions: list[str] = []
    for kind, wanted in (("Deployment", deployment(d)), ("Service", service(d))):
        current = api.get(kind, d.namespace, d.name)
        if current is None:
            api.create(kind, d.namespace, wanted)
            actions.append(f"created {kind}")
            continue
        if not _owned_by(current, d):
            raise ValueError(f"refusing to take over unowned {kind} {d.namespace}/{d.name}")
        if _drifted(kind, current, wanted):
            patch = {"spec": wanted["spec"]}
            if kind == "Deployment":
                # Deployment selectors are immutable; existing owned selectors stay fixed.
                patch["spec"] = {"replicas": d.replicas, "template": wanted["spec"]["template"]}
            else:
                # JSON merge-patch retains unspecified map keys; null removes stale selectors.
                stale = set(current.get("spec", {}).get("selector", {})) - set(wanted["spec"]["selector"])
                patch["spec"] = {**wanted["spec"], "selector": {
                    **wanted["spec"]["selector"], **{key: None for key in stale},
                }}
            api.patch(kind, d.namespace, d.name, patch)
            actions.append(f"patched {kind}")
    return actions
