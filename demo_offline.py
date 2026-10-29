"""Thirty deterministic fault cases through the real reconcile core, offline."""
from __future__ import annotations

import json
import platform
import time
from pathlib import Path

from controller import reconcile
from offline_api import MemoryAPI
from test_controller import RESOURCE


def main() -> None:
    api = MemoryAPI()
    cases = ["delete_deployment", "delete_service", "image_drift", "replica_drift", "service_port_drift", "selector_drift"] * 5
    elapsed_ms = []
    for fault in cases:
        reconcile(api, RESOURCE)
        dep = api.objects[("Deployment", "default", "demo-inference")]
        svc = api.objects[("Service", "default", "demo-inference")]
        if fault == "delete_deployment":
            api.delete("Deployment", "default", "demo-inference")
        elif fault == "delete_service":
            api.delete("Service", "default", "demo-inference")
        elif fault == "image_drift":
            dep["spec"]["template"]["spec"]["containers"][0]["image"] = "wrong:tag"
        elif fault == "replica_drift":
            dep["spec"]["replicas"] = 9
        elif fault == "service_port_drift":
            svc["spec"]["ports"][0]["port"] = 81
        else:
            svc["spec"]["selector"] = {"wrong": "label"}
        start = time.perf_counter()
        actions = reconcile(api, RESOURCE)
        elapsed_ms.append((time.perf_counter() - start) * 1000)
        assert actions and reconcile(api, RESOURCE) == [], fault
        assert len(api.objects) == 2, fault
    result = {
        "scope": "in-memory API only; no live Kubernetes latency claim",
        "environment": {"python": platform.python_version(), "platform": platform.platform()},
        "cases": len(cases), "repaired": len(cases), "unique_objects": len(api.objects),
        "max_reconcile_ms_in_memory": round(max(elapsed_ms), 3),
        "creates": api.creates, "patches": api.patches,
        "baseline": "a one-shot manifest has no controller reconciliation after fault injection",
    }
    Path("results-offline.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
