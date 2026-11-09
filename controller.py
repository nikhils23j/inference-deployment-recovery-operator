"""Reconcile logic for the InferenceService custom resource (controller v2).

The reconciler is written against a tiny API interface (see ``ResourceAPI``) so
the exact same code runs against:
  * ``offline_api.FakeAPIServer`` - a simulated-clock fake used for chaos tests;
  * ``k8s_adapter.KubernetesAPI`` - the official Kubernetes Python client.

It never serves a model; the "inference" Deployment is just an image + port.
The v1 code that produced the old live k3s numbers is kept in ``v1/``.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable, Protocol

GROUP = "portfolio.nikhil.dev"
VERSION = "v1alpha1"
PLURAL = "inferenceservices"
KIND = "InferenceService"
FINALIZER = "portfolio.nikhil.dev/cleanup"
MANAGED_BY = "inference-recovery-operator"
CHILD_KINDS = ("Deployment", "Service")


# --------------------------------------------------------------------------- errors
class ApiError(Exception):
    status = 500
    reason = "InternalError"

    def __init__(self, message: str = "", status: int | None = None):
        super().__init__(message or self.reason)
        if status is not None:
            self.status = status


class NotFound(ApiError):
    status, reason = 404, "NotFound"


class Conflict(ApiError):
    """409 because the resourceVersion precondition failed (stale read)."""
    status, reason = 409, "Conflict"


class AlreadyExists(ApiError):
    status, reason = 409, "AlreadyExists"


class Invalid(ApiError):
    status, reason = 422, "Invalid"


class ServerError(ApiError):
    status, reason = 503, "ServiceUnavailable"


class ResourceAPI(Protocol):
    """Writes always go to the API server; resourceVersion in the body is a precondition."""
    def get(self, kind: str, namespace: str, name: str) -> dict | None: ...
    def list(self, kind: str, namespace: str) -> list[dict]: ...
    def create(self, kind: str, namespace: str, body: dict) -> dict: ...
    def update(self, kind: str, namespace: str, body: dict) -> dict: ...
    def update_status(self, kind: str, namespace: str, body: dict) -> dict: ...
    def delete(self, kind: str, namespace: str, name: str, uid: str | None = None) -> None: ...


class Reader(Protocol):
    def get(self, kind: str, namespace: str, name: str) -> dict | None: ...


# --------------------------------------------------------------------------- desired state
@dataclass(frozen=True)
class Desired:
    namespace: str
    name: str
    uid: str
    image: str
    replicas: int
    port: int
    progress_deadline: int = 120

    @classmethod
    def from_resource(cls, resource: dict) -> "Desired":
        meta = resource["metadata"]
        spec = resource.get("spec") or {}
        image = spec.get("image")
        replicas = spec.get("replicas", 1)
        port = spec.get("port", 8080)
        deadline = spec.get("progressDeadlineSeconds", 120)
        if not isinstance(image, str) or not image.strip():
            raise ValueError("spec.image must be a nonempty string")
        if type(replicas) is not int or not 0 <= replicas <= 10:
            raise ValueError("spec.replicas must be an integer from 0 to 10")
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("spec.port must be an integer from 1 to 65535")
        if type(deadline) is not int or not 5 <= deadline <= 3600:
            raise ValueError("spec.progressDeadlineSeconds must be an integer from 5 to 3600")
        return cls(meta["namespace"], meta["name"], meta["uid"], image, replicas, port, deadline)


def _labels(d: Desired) -> dict:
    return {"app.kubernetes.io/name": d.name, "app.kubernetes.io/managed-by": MANAGED_BY}


def owner_reference(d: Desired) -> dict:
    return {"apiVersion": f"{GROUP}/{VERSION}", "kind": KIND, "name": d.name, "uid": d.uid,
            "controller": True, "blockOwnerDeletion": True}


def selector(d: Desired) -> dict:
    return {"app.kubernetes.io/name": d.name}


def desired_deployment(d: Desired, image: str) -> dict:
    return {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": d.name, "namespace": d.namespace, "labels": _labels(d),
                     "ownerReferences": [owner_reference(d)]},
        "spec": {
            "replicas": d.replicas,
            "progressDeadlineSeconds": d.progress_deadline,
            "selector": {"matchLabels": selector(d)},
            "template": {
                "metadata": {"labels": selector(d)},
                "spec": {"containers": [{"name": "model-server", "image": image,
                                         "ports": [{"containerPort": d.port}]}]},
            },
        },
    }


def desired_service(d: Desired) -> dict:
    return {
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": d.name, "namespace": d.namespace, "labels": _labels(d),
                     "ownerReferences": [owner_reference(d)]},
        "spec": {"type": "ClusterIP", "selector": selector(d),
                 "ports": [{"name": "http", "port": d.port, "targetPort": d.port, "protocol": "TCP"}]},
    }


def controller_owner_uid(obj: dict) -> str | None:
    for ref in (obj.get("metadata") or {}).get("ownerReferences") or []:
        if ref.get("kind") == KIND and ref.get("controller"):
            return ref.get("uid")
    return None


def owned_by(obj: dict, uid: str) -> bool:
    return controller_owner_uid(obj) == uid


def container_image(dep: dict) -> str | None:
    containers = (((dep.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
    return containers[0].get("image") if containers else None


def deployment_drift(cur: dict, d: Desired, image: str) -> list[str]:
    """Return the managed fields that differ. Unmanaged fields are ignored on purpose."""
    spec = cur.get("spec") or {}
    containers = ((spec.get("template") or {}).get("spec") or {}).get("containers") or []
    out = []
    if spec.get("replicas") != d.replicas:
        out.append("replicas")
    if spec.get("progressDeadlineSeconds") != d.progress_deadline:
        out.append("progressDeadlineSeconds")
    if len(containers) != 1:
        return out + ["containers"]
    if containers[0].get("image") != image:
        out.append("image")
    ports = containers[0].get("ports") or [{}]
    if len(ports) != 1 or ports[0].get("containerPort") != d.port:
        out.append("containerPort")
    labels = ((spec.get("template") or {}).get("metadata") or {}).get("labels") or {}
    if any(labels.get(k) != v for k, v in selector(d).items()):
        out.append("podLabels")
    return out


def service_drift(cur: dict, d: Desired) -> list[str]:
    spec = cur.get("spec") or {}
    ports = spec.get("ports") or []
    out = []
    if spec.get("selector") != selector(d):
        out.append("selector")
    if len(ports) != 1 or ports[0].get("port") != d.port or ports[0].get("targetPort") != d.port:
        out.append("ports")
    return out


def apply_deployment(cur: dict, d: Desired, image: str) -> dict:
    """Overwrite only the fields this controller owns; keep resourceVersion as precondition."""
    new = deepcopy(cur)
    want = desired_deployment(d, image)["spec"]
    spec = new.setdefault("spec", {})
    spec["replicas"] = want["replicas"]
    spec["progressDeadlineSeconds"] = want["progressDeadlineSeconds"]
    tmpl = spec.setdefault("template", {})
    tmpl.setdefault("metadata", {}).setdefault("labels", {}).update(selector(d))
    old = ((tmpl.get("spec") or {}).get("containers") or [{}])[0]
    container = {**old, **want["template"]["spec"]["containers"][0]}
    tmpl.setdefault("spec", {})["containers"] = [container]
    new.pop("status", None)
    return new


def apply_service(cur: dict, d: Desired) -> dict:
    new = deepcopy(cur)
    want = desired_service(d)["spec"]
    new.setdefault("spec", {})["selector"] = want["selector"]
    new["spec"]["ports"] = want["ports"]
    return new


def rollout_state(dep: dict) -> str:
    """'complete' | 'failed' | 'progressing', judged only on status for the current generation."""
    status = dep.get("status") or {}
    if status.get("observedGeneration", 0) < (dep.get("metadata") or {}).get("generation", 0):
        return "progressing"
    for cond in status.get("conditions") or []:
        if cond.get("type") == "Progressing" and cond.get("reason") == "ProgressDeadlineExceeded":
            return "failed"
    want = (dep.get("spec") or {}).get("replicas", 1)
    if status.get("updatedReplicas", 0) >= want and status.get("availableReplicas", 0) >= want:
        return "complete"
    return "progressing"


def set_condition(conditions: list[dict], ctype: str, status: str, reason: str, message: str,
                  now: str, generation: int) -> list[dict]:
    """Upsert a condition; lastTransitionTime only moves when status flips (avoids status hot-loops)."""
    out, found = [], False
    for c in conditions:
        if c.get("type") == ctype:
            found = True
            ltt = c.get("lastTransitionTime") if c.get("status") == status else now
            out.append({"type": ctype, "status": status, "reason": reason, "message": message,
                        "lastTransitionTime": ltt, "observedGeneration": generation})
        else:
            out.append(c)
    if not found:
        out.append({"type": ctype, "status": status, "reason": reason, "message": message,
                    "lastTransitionTime": now, "observedGeneration": generation})
    return out


def condition(obj: dict, ctype: str) -> dict | None:
    for c in (obj.get("status") or {}).get("conditions") or []:
        if c.get("type") == ctype:
            return c
    return None


# --------------------------------------------------------------------------- reconciler
@dataclass
class Result:
    actions: list[str] = field(default_factory=list)
    requeue_after: float | None = None


class Reconciler:
    """Level-triggered: every call looks at the whole current state, never at the event."""

    BLOCKED_REQUEUE_S = 30.0

    def __init__(self, api: ResourceAPI, reader: Reader | None = None,
                 now: Callable[[], str] = lambda: "1970-01-01T00:00:00Z"):
        self.api = api
        self.reader = reader or api
        self.now = now

    def reconcile(self, namespace: str, name: str) -> Result:
        res = Result()
        cr = self.reader.get(KIND, namespace, name)
        if cr is None:
            return res  # deleted; ownerReference GC removes anything we missed
        if (cr.get("metadata") or {}).get("deletionTimestamp"):
            self._finalize(cr, res)
            return res
        if FINALIZER not in (cr["metadata"].get("finalizers") or []):
            body = deepcopy(cr)
            body["metadata"]["finalizers"] = [*(cr["metadata"].get("finalizers") or []), FINALIZER]
            body.pop("status", None)
            updated = self.api.update(KIND, namespace, body)
            cr = {**updated, "status": cr.get("status") or {}}
            res.actions.append("added finalizer")

        generation = cr["metadata"].get("generation", 1)
        old_status = deepcopy(cr.get("status") or {})
        status = deepcopy(old_status)
        conds = status.get("conditions") or []
        now = self.now()
        try:
            d = Desired.from_resource(cr)
        except ValueError as exc:  # CRD validation should stop this; be defensive anyway
            conds = set_condition(conds, "Ready", "False", "InvalidSpec", str(exc), now, generation)
            conds = set_condition(conds, "Degraded", "True", "InvalidSpec", str(exc), now, generation)
            status.update(conditions=conds, observedGeneration=generation)
            self._write_status(cr, old_status, status, res)
            return res

        observed = {kind: self.reader.get(kind, namespace, name) for kind in CHILD_KINDS}
        seen = observed["Deployment"]
        # Record "last good" from what is running *before* we change it. Waiting until after our own
        # update (the first version) lost it whenever we missed the moment a rollout finished.
        if (seen is not None and owned_by(seen, d.uid) and rollout_state(seen) == "complete"
                and container_image(seen) == status.get("appliedImage")):
            status["lastGoodImage"] = status["appliedImage"]
        last_good, failed = status.get("lastGoodImage"), status.get("failedImage")
        rolled_back = failed == d.image and bool(last_good) and last_good != d.image
        image = last_good if rolled_back else d.image

        blocked: list[str] = []
        children: dict[str, dict | None] = {}
        for kind in CHILD_KINDS:
            cur = observed[kind]
            if cur is None:
                body = desired_deployment(d, image) if kind == "Deployment" else desired_service(d)
                cur = self.api.create(kind, namespace, body)  # AlreadyExists -> stale cache -> retry
                res.actions.append(f"created {kind}")
                if kind == "Deployment":
                    status["appliedImage"] = image
            elif not owned_by(cur, d.uid):
                blocked.append(kind)  # never touch objects we do not own
                cur = None
            else:
                drift = deployment_drift(cur, d, image) if kind == "Deployment" else service_drift(cur, d)
                if drift:
                    body = apply_deployment(cur, d, image) if kind == "Deployment" else apply_service(cur, d)
                    cur = self.api.update(kind, namespace, body)  # Conflict -> retry with backoff
                    res.actions.append(f"patched {kind} ({','.join(drift)})")
                    if kind == "Deployment":
                        status["appliedImage"] = image
            children[kind] = cur

        dep = children["Deployment"]
        state = rollout_state(dep) if dep else None
        if dep is not None and container_image(dep) == image:
            if state == "complete":
                status["lastGoodImage"] = image
                if image == d.image:
                    status.pop("failedImage", None)
            elif state == "failed" and image == d.image and failed != d.image:
                status["failedImage"] = d.image
                res.actions.append(f"rollout of {d.image} failed")
                if last_good and last_good != d.image:
                    res.requeue_after = 0.0  # roll back on the next pass

        if blocked:
            msg = f"refusing to adopt unowned {'/'.join(blocked)} named {name}"
            conds = set_condition(conds, "Degraded", "True", "ChildNotOwned", msg, now, generation)
            conds = set_condition(conds, "Ready", "False", "ChildNotOwned", msg, now, generation)
            res.requeue_after = self.BLOCKED_REQUEUE_S
        elif status.get("failedImage") == d.image:
            if status.get("lastGoodImage") and status["lastGoodImage"] != d.image:
                msg = f"{d.image} missed its progress deadline; serving {status['lastGoodImage']}"
                conds = set_condition(conds, "Degraded", "True", "RolledBack", msg, now, generation)
                conds = set_condition(conds, "Ready", "False", "RolledBack", msg, now, generation)
            else:
                msg = f"{d.image} missed its progress deadline and there is no previous good image"
                conds = set_condition(conds, "Degraded", "True", "RolloutFailed", msg, now, generation)
                conds = set_condition(conds, "Ready", "False", "RolloutFailed", msg, now, generation)
        else:
            conds = set_condition(conds, "Degraded", "False", "AsExpected", "", now, generation)
            if state == "complete":
                conds = set_condition(conds, "Ready", "True", "Available", "", now, generation)
            else:
                conds = set_condition(conds, "Ready", "False", "RollingOut",
                                      f"waiting for {image} to become available", now, generation)
        progressing = state == "progressing" and not blocked
        conds = set_condition(conds, "Progressing", "True" if progressing else "False",
                              "RollingOut" if progressing else "Idle", "", now, generation)
        status.update(conditions=conds, observedGeneration=generation,
                      currentImage=container_image(dep) if dep else None)
        self._write_status(cr, old_status, status, res)
        return res

    def _write_status(self, cr: dict, old: dict, new: dict, res: Result) -> None:
        if new == old:
            return  # an unchanged status write would trigger another event -> hot loop
        body = deepcopy(cr)
        body["status"] = new
        self.api.update_status(KIND, cr["metadata"]["namespace"], body)
        res.actions.append("updated status")

    def _finalize(self, cr: dict, res: Result) -> None:
        meta = cr["metadata"]
        if FINALIZER not in (meta.get("finalizers") or []):
            return
        for kind in CHILD_KINDS:
            child = self.api.get(kind, meta["namespace"], meta["name"])  # fresh read, not cache
            if child is not None and owned_by(child, meta["uid"]):
                try:
                    self.api.delete(kind, meta["namespace"], meta["name"], uid=child["metadata"]["uid"])
                    res.actions.append(f"deleted {kind}")
                except NotFound:
                    pass
        body = deepcopy(cr)
        body["metadata"]["finalizers"] = [f for f in meta["finalizers"] if f != FINALIZER]
        body.pop("status", None)
        try:
            self.api.update(KIND, meta["namespace"], body)
        except NotFound:
            return  # already gone: our cache was behind, nothing left to do
        res.actions.append("removed finalizer")
