"""Baseline controllers for comparison.

* ``v1``   - the exact reconcile() that produced the old live k3s numbers (kept
             verbatim in v1/controller_v1.py): polls every 0.5 s with direct GETs,
             checks ownership, blind patches, no status/finalizer/leases/backoff.
* ``naive`` - what you might write first: polls every 0.5 s, NO ownership check
             (it adopts whatever has the right name), blind patches, retries a
             failure after 10 ms with no backoff, and runs two replicas with no
             leader election.
"""
from __future__ import annotations

from controller import AlreadyExists, Desired, desired_deployment, desired_service, deployment_drift, service_drift
from v1.controller_v1 import reconcile as v1_reconcile  # noqa: F401  (re-exported)


class V1Adapter:
    """v1's three-method interface on top of a fake-API client (direct reads, blind merge patches)."""

    def __init__(self, client):
        self.client = client

    def get(self, kind, namespace, name):
        return self.client.get(kind, namespace, name)

    def create(self, kind, namespace, body):
        try:
            self.client.create(kind, namespace, body)
        except AlreadyExists:
            pass  # v1's live adapter swallowed 409 on create too

    def patch(self, kind, namespace, name, body):
        self.client.patch(kind, namespace, name, body)


def naive_reconcile(client, resource: dict) -> list[str]:
    d = Desired.from_resource(resource)
    actions = []
    for kind in ("Deployment", "Service"):
        want = desired_deployment(d, d.image) if kind == "Deployment" else desired_service(d)
        cur = client.get(kind, d.namespace, d.name)
        if cur is None:
            try:
                client.create(kind, d.namespace, want)
                actions.append(f"created {kind}")
            except AlreadyExists:
                pass
            continue
        drift = deployment_drift(cur, d, d.image) if kind == "Deployment" else service_drift(cur, d)
        if drift or cur["metadata"].get("ownerReferences") != want["metadata"]["ownerReferences"]:
            patch = {"metadata": {"ownerReferences": want["metadata"]["ownerReferences"]}}
            if kind == "Deployment":
                patch["spec"] = {"replicas": d.replicas, "progressDeadlineSeconds": d.progress_deadline,
                                 "template": want["spec"]["template"]}
            else:
                stale = set((cur.get("spec") or {}).get("selector") or {}) - set(want["spec"]["selector"])
                patch["spec"] = {**want["spec"], "selector": {**want["spec"]["selector"], **{k: None for k in stale}}}
            client.patch(kind, d.namespace, d.name, patch)  # no ownership check, no precondition
            actions.append(f"patched {kind}")
    return actions
