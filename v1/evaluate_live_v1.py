"""Thirty fault injections against a real local Kubernetes API server.

Requires the CRD and sample resource to be applied first. The evaluation measures
object-spec convergence, not Pod readiness or inference serving availability.
"""
from __future__ import annotations

import argparse
import json
import platform
import threading
import time
from pathlib import Path

from controller import Desired, _drifted, deployment, reconcile, service
from k8s_adapter import KubernetesAPI


FAULTS = ("delete_deployment", "delete_service", "image_drift", "replica_drift", "service_port_drift", "selector_drift")


def healthy(api: KubernetesAPI, d: Desired) -> bool:
    dep = api.get("Deployment", d.namespace, d.name)
    svc = api.get("Service", d.namespace, d.name)
    return bool(dep and svc and not _drifted("Deployment", dep, deployment(d))
                and not _drifted("Service", svc, service(d)))


def owned_count(api: KubernetesAPI, d: Desired) -> int:
    deployments = api.apps.list_namespaced_deployment(d.namespace).items
    services = api.core.list_namespaced_service(d.namespace).items
    return sum(1 for obj in [*deployments, *services]
               if any(ref.uid == d.uid for ref in (obj.metadata.owner_references or [])))


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
    parser.add_argument("--output", default="results-live.json")
    args = parser.parse_args()
    api = KubernetesAPI()
    resources = {r["metadata"]["name"]: r for r in api.resources(args.namespace)}
    if args.name not in resources:
        raise SystemExit("apply crd.yaml and sample.yaml before live evaluation")
    resource = resources[args.name]
    d = Desired.from_resource(resource)
    stop = threading.Event()
    errors = []

    def controller_loop():
        while not stop.is_set():
            try:
                reconcile(api, resource)
            except Exception as exc:
                errors.append(repr(exc))
            stop.wait(0.5)

    thread = threading.Thread(target=controller_loop, daemon=True)
    thread.start()
    records = []
    max_owned_count = 0
    try:
        if wait_healthy(api, d, 30) is None:
            raise RuntimeError("initial objects did not converge")
        for fault in FAULTS * 5:
            if not healthy(api, d):
                raise RuntimeError("objects drifted before next fault")
            inject(api, d, fault)
            elapsed = wait_healthy(api, d, 30)
            max_owned_count = max(max_owned_count, owned_count(api, d))
            records.append({"fault": fault, "recovered": elapsed is not None,
                            "recovery_s": round(elapsed, 3) if elapsed is not None else None})
            if elapsed is None:
                break
    finally:
        stop.set()
        thread.join(timeout=5)
    # One-shot baseline: with the custom controller stopped, the Deployment
    # object is not restored by Kubernetes' own Deployment controller.
    api.apps.delete_namespaced_deployment(d.name, d.namespace)
    time.sleep(3)
    baseline_recreated = api.get("Deployment", d.namespace, d.name) is not None
    reconcile(api, resource)  # restore the sample after baseline observation
    passed = sum(r["recovered"] and r["recovery_s"] <= 30 for r in records)
    result = {
        "scope": "live local Kubernetes API object convergence; not Pod readiness",
        "environment": {"python": platform.python_version(), "platform": platform.platform()},
        "resource": f"{d.namespace}/{d.name}", "cases_run": len(records),
        "recovered_within_30s": passed,
        "max_recovery_s": max((r["recovery_s"] for r in records if r["recovered"]), default=None),
        "max_owned_deployments_and_services": max_owned_count,
        "controller_errors": errors,
        "one_shot_baseline_recreated_deployment_after_3s": baseline_recreated,
        "target_met": len(records) == 30 and passed >= 29 and max_owned_count == 2
                      and not baseline_recreated and not errors,
        "records": records,
    }
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
