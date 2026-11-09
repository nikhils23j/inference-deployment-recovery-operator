"""Offline demo: the same 30 deletion/drift faults the live evaluator injects, replayed
in the simulator against v1 and v2, plus a narrated broken-image rollback.

    python3 demo_offline.py            # writes results-offline.json

Times are SIMULATED seconds (assumed 1-5 ms API latency, 0-50 ms watch delay).
They are not comparable to results-live.json, which came from a real k3s cluster.
"""
from __future__ import annotations

import json
import platform
import random
import statistics
from pathlib import Path

from chaos import INITIAL_SPEC, NAME, NS, healthy
from controller import KIND, condition, container_image
from offline_api import FakeAPIServer, FakeDeploymentController, Sim
from simulation import Metrics, PollerReplica, SimConfig, V2Replica

FAULTS = ("delete_deployment", "delete_service", "image_drift", "replica_drift", "service_port_drift", "selector_drift")


def world(variant: str, seed: int = 0):
    sim = Sim()
    server = FakeAPIServer(sim, random.Random(seed))
    FakeDeploymentController(sim, server)
    user = server.client("user")
    cr = user.create(KIND, NS, {"metadata": {"name": NAME}, "spec": dict(INITIAL_SPEC)})
    cfg, metrics = SimConfig(), Metrics()
    if variant == "v2":
        reps = [V2Replica(sim, server, f"ctrl-{c}", random.Random(seed + i), cfg, metrics) for i, c in enumerate("ab")]
    else:
        reps = [PollerReplica(sim, server, "ctrl-a", cfg, metrics, "v1")]
    for i, r in enumerate(reps):
        sim.at(0.01 * i, r.start)
    return sim, server, user, cr["metadata"]["uid"], reps


def inject(server, user, fault):
    if fault == "delete_deployment":
        user.delete("Deployment", NS, NAME)
    elif fault == "delete_service":
        user.delete("Service", NS, NAME)
    elif fault == "image_drift":
        c = dict(server.raw("Deployment", NS, NAME)["spec"]["template"]["spec"]["containers"][0], image="nginx:wrong")
        user.patch("Deployment", NS, NAME, {"spec": {"template": {"spec": {"containers": [c]}}}})
    elif fault == "replica_drift":
        user.patch("Deployment", NS, NAME, {"spec": {"replicas": 9}})
    elif fault == "service_port_drift":
        user.patch("Service", NS, NAME, {"spec": {"ports": [{"name": "http", "port": 81, "targetPort": 81}]}})
    else:
        user.patch("Service", NS, NAME, {"spec": {"selector": {"app.kubernetes.io/name": None, "wrong": "label"}}})


def objects_ok(server, uid, check_status):
    """Spec-level repair (same notion as the live evaluator): ignore rollout availability."""
    from evaluate_live import objects_match
    from controller import Desired
    cr = server.raw(KIND, NS, NAME)
    ok = objects_match(server.raw("Deployment", NS, NAME), server.raw("Service", NS, NAME), Desired.from_resource(cr))
    return ok and (not check_status or (cr.get("status") or {}).get("observedGeneration") == cr["metadata"]["generation"])


def classic_30(variant: str) -> dict:
    sim, server, user, uid, _ = world(variant)
    sim.run_until(10.0)
    records = []
    for i, fault in enumerate(FAULTS * 5):
        t0 = 10.0 + 10.0 * i
        sim.run_until(t0)
        assert healthy(server, uid, False, variant == "v2"), f"not healthy before {fault}"
        inject(server, user, fault)
        repaired = None
        while sim._heap and sim._heap[0][0] <= t0 + 9.0:
            sim.run_until(sim._heap[0][0])
            if objects_ok(server, uid, False):
                repaired = sim.now - t0
                break
        records.append({"fault": fault, "repaired": repaired is not None,
                        "sim_repair_s": round(repaired, 4) if repaired is not None else None})
    reqs = sum(1 for a in server.audit if a.actor.startswith("ctrl-"))
    times = [r["sim_repair_s"] for r in records if r["repaired"]]
    return {"cases": len(records), "repaired": len(times),
            "sim_repair_s_median": round(statistics.median(times), 4), "sim_repair_s_max": max(times),
            "controller_requests_total": reqs, "sim_seconds": round(sim.now, 1),
            "controller_requests_per_sim_s": round(reqs / sim.now, 2), "records": records}


def narrated_rollback() -> list[str]:
    sim, server, user, uid, reps = world("v2")
    log = []

    def show(label):
        cr, dep = server.raw(KIND, NS, NAME), server.raw("Deployment", NS, NAME)
        ready = condition(cr, "Ready") or {}
        log.append(f"t={sim.now:6.2f}s  {label:<44} spec={cr['spec']['image']:<32} "
                   f"deploy_image={container_image(dep) if dep else '-':<26} Ready={ready.get('status')}/{ready.get('reason')}")
    sim.run_until(10)
    show("initial rollout done")
    user.patch(KIND, NS, NAME, {"spec": {"image": "registry.local/model:v2-broken"}})
    sim.run_until(15)
    show("user pushes a broken image")
    sim.run_until(45)
    show("after 20 s progress deadline")
    user.delete("Deployment", NS, NAME)
    sim.run_until(46)
    show("someone deletes the Deployment")
    leader = next(r for r in reps if r.leading)
    leader.crash()
    user.patch(KIND, NS, NAME, {"spec": {"image": "registry.local/model:v3"}})
    sim.run_until(50)
    show(f"leader {leader.identity} crashes + user fixes image")
    sim.run_until(75)
    show("standby took the Lease and rolled forward")
    return log


def main() -> None:
    log = narrated_rollback()
    print("\n".join(log))
    result = {
        "scope": ("OFFLINE SIMULATION (fake API server, simulated clock). Repair times are simulated seconds under "
                  "assumed latencies and are NOT Kubernetes measurements; see results-live.json for the real k3s run."),
        "command": "python3 demo_offline.py",
        "environment": {"python": platform.python_version(), "platform": platform.platform()},
        "classic_30_faults": {"v1": classic_30("v1"), "v2": classic_30("v2")},
        "narrated_rollback": log,
    }
    Path("results-offline.json").write_text(json.dumps(result, indent=2) + "\n")
    for v in ("v1", "v2"):
        c = result["classic_30_faults"][v]
        print(f"{v}: repaired {c['repaired']}/{c['cases']}  median {c['sim_repair_s_median']} sim-s  "
              f"max {c['sim_repair_s_max']} sim-s  controller requests/sim-s {c['controller_requests_per_sim_s']}")


if __name__ == "__main__":
    main()
