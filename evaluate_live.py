"""Thirty fault injections against a REAL Kubernetes API server (k3s / kind / minikube).

Requires crd.yaml and sample.yaml to be applied first, and the standalone operator
to be stopped. Measures API-object convergence, not Pod readiness.

    .venv/bin/python evaluate_live.py --controller v2   # writes results-live-v2.json
    .venv/bin/python evaluate_live.py --controller v1   # re-runs the original v1 loop

results-live.json (30 Oct 2026, v1 controller, Lima/k3s) is the historical run and is
never overwritten by default.
"""
from __future__ import annotations

import argparse
import json
import platform
import threading
import time
from pathlib import Path

from controller import Desired, controller_owner_uid
from k8s_adapter import KubernetesAPI, LiveOperator

FAULTS = ("delete_deployment", "delete_service", "image_drift", "replica_drift", "service_port_drift", "selector_drift")


def objects_match(dep: dict | None, svc: dict | None, d: Desired) -> bool:
    """Spec-level check shared by v1 and v2 (ignores v2-only fields like progressDeadlineSeconds)."""
    if not dep or not svc or controller_owner_uid(dep) != d.uid or controller_owner_uid(svc) != d.uid:
        return False
    containers = dep["spec"]["template"]["spec"]["containers"]
    ports = svc["spec"].get("ports") or []
    return (dep["spec"].get("replicas") == d.replicas and len(containers) == 1
            and containers[0].get("image") == d.image
            and (containers[0].get("ports") or [{}])[0].get("containerPort") == d.port
            and svc["spec"].get("selector") == {"app.kubernetes.io/name": d.name}
            and len(ports) == 1 and ports[0].get("port") == d.port and ports[0].get("targetPort") == d.port)


def healthy(api: KubernetesAPI, d: Desired) -> bool:
    return objects_match(api.get("Deployment", d.namespace, d.name), api.get("Service", d.namespace, d.name), d)


def owned_count(api: KubernetesAPI, d: Desired) -> int:
    objs = api.list("Deployment", d.namespace) + api.list("Service", d.namespace)
    return sum(1 for o in objs if controller_owner_uid(o) == d.uid)


def inject(api: KubernetesAPI, d: Desired, fault: str) -> None:
    if fault == "delete_deployment":
        api.apps.delete_namespaced_deployment(d.name, d.namespace)
    elif fault == "delete_service":
        api.core.delete_namespaced_service(d.name, d.namespace)
    elif fault == "image_drift":
        api.apps.patch_namespaced_deployment(d.name, d.namespace, [
            {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": "nginx:wrong"}])
    elif fault == "replica_drift":
        api.apps.patch_namespaced_deployment(d.name, d.namespace, [
            {"op": "replace", "path": "/spec/replicas", "value": 9}])
    elif fault == "service_port_drift":
        api.core.patch_namespaced_service(d.name, d.namespace, [
            {"op": "replace", "path": "/spec/ports/0/port", "value": 81},
            {"op": "replace", "path": "/spec/ports/0/targetPort", "value": 81}])
    else:
        api.core.patch_namespaced_service(d.name, d.namespace, [
            {"op": "replace", "path": "/spec/selector", "value": {"wrong": "label"}}])


class V1LiveAdapter:
    """The v1 adapter's get/create/patch behaviour (JSON-patch replace), on the new client wrapper."""

    def __init__(self, api: KubernetesAPI):
        self.api = api

    def get(self, kind, namespace, name):
        return self.api.get(kind, namespace, name)

    def create(self, kind, namespace, body):
        from controller import AlreadyExists
        try:
            self.api.create(kind, namespace, body)
        except AlreadyExists:
            pass

    def patch(self, kind, namespace, name, body):
        spec = body["spec"]
        if kind == "Deployment":
            ops = [{"op": "replace", "path": "/spec/replicas", "value": spec["replicas"]},
                   {"op": "replace", "path": "/spec/template", "value": spec["template"]}]
            self.api.apps.patch_namespaced_deployment(name, namespace, ops)
        else:
            selector = {k: v for k, v in spec["selector"].items() if v is not None}
            ops = [{"op": "replace", "path": "/spec/selector", "value": selector},
                   {"op": "replace", "path": "/spec/ports", "value": spec["ports"]}]
            self.api.core.patch_namespaced_service(name, namespace, ops)


def wait_healthy(api: KubernetesAPI, d: Desired, timeout: float) -> float | None:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if healthy(api, d):
            return time.monotonic() - start
        time.sleep(0.1)
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--name", default="demo-inference")
    parser.add_argument("--controller", choices=["v1", "v2"], default="v2")
    parser.add_argument("--output", default=None, help="default: results-live-<controller>-rerun.json")
    args = parser.parse_args()
    output = args.output or f"results-live-{args.controller}-rerun.json"
    if Path(output).name == "results-live.json":
        raise SystemExit("refusing to overwrite the historical results-live.json; pick another --output")
    api = KubernetesAPI()
    resources = {r["metadata"]["name"]: r for r in api.list("InferenceService", args.namespace)}
    if args.name not in resources:
        raise SystemExit("apply crd.yaml and sample.yaml before live evaluation")
    resource = resources[args.name]
    d = Desired.from_resource(resource)
    errors: list[str] = []
    stop = threading.Event()

    if args.controller == "v2":
        op = LiveOperator(api, args.namespace, leader_elect=True, identity="evaluate-live")
        op.run()
        stop = op.stop
    else:
        from v1.controller_v1 import reconcile as v1_reconcile
        adapter = V1LiveAdapter(api)

        def loop():
            while not stop.is_set():
                try:
                    v1_reconcile(adapter, resource)
                except Exception as exc:  # noqa: BLE001 - recorded, like the original evaluator
                    errors.append(repr(exc))
                stop.wait(0.5)
        threading.Thread(target=loop, daemon=True).start()

    records, max_owned = [], 0
    try:
        if wait_healthy(api, d, 60) is None:
            raise RuntimeError("initial objects did not converge")
        for fault in FAULTS * 5:
            if not healthy(api, d):
                raise RuntimeError("objects drifted before next fault")
            inject(api, d, fault)
            elapsed = wait_healthy(api, d, 30)
            max_owned = max(max_owned, owned_count(api, d))
            records.append({"fault": fault, "recovered": elapsed is not None,
                            "recovery_s": round(elapsed, 3) if elapsed is not None else None})
            if elapsed is None:
                break
    finally:
        stop.set()
        time.sleep(1.0)

    # Baseline: with the controller stopped nothing restores a deleted Deployment object.
    api.apps.delete_namespaced_deployment(d.name, d.namespace)
    time.sleep(3)
    baseline_recreated = api.get("Deployment", d.namespace, d.name) is not None
    restore = LiveOperator(api, args.namespace, leader_elect=False)
    restore.reconciler.reconcile(d.namespace, d.name)  # put the sample back
    passed = sum(r["recovered"] and r["recovery_s"] <= 30 for r in records)
    rec = [r["recovery_s"] for r in records if r["recovered"]]
    result = {
        "scope": "live local Kubernetes API object convergence; not Pod readiness",
        "controller": args.controller,
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "date": time.strftime("%Y-%m-%d")},
        "resource": f"{d.namespace}/{d.name}", "cases_run": len(records),
        "recovered_within_30s": passed,
        "median_recovery_s": sorted(rec)[len(rec) // 2] if rec else None,
        "max_recovery_s": max(rec, default=None),
        "max_owned_deployments_and_services": max_owned,
        "controller_errors": errors,
        "one_shot_baseline_recreated_deployment_after_3s": baseline_recreated,
        "records": records,
    }
    Path(output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
