"""Randomized fault schedules + invariant checker, run against every controller variant.

    python3 chaos.py --seeds 300 --output results-chaos.json

Every number this prints is SIMULATED time on the fake API server in offline_api.py
(assumed 1-5 ms request latency, 1-4 s rollouts, etc.). It is evidence about the
control logic under adversarial interleavings, not about Kubernetes latency.
"""
from __future__ import annotations

import argparse
import json
import platform
import random
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from controller import (KIND, AlreadyExists, ApiError, NotFound, condition, container_image,
                        controller_owner_uid, desired_deployment, desired_service, Desired, rollout_state)
from offline_api import ChaosConfig, FakeAPIServer, FakeDeploymentController, Sim
from simulation import Metrics, PollerReplica, SimConfig, V2Replica

NS, NAME = "default", "demo-inference"
VARIANTS = ("none", "naive", "v1", "v2", "v2_no_lease", "v2_no_refresh")
CONTROLLER_PREFIX = "ctrl-"
WRITE_VERBS = ("create", "update", "update_status", "delete", "patch")
WARMUP = 10.0              # faults start at t>=10 s; unhealthy time is integrated from here
QUIET_TAIL = 150.0         # simulated seconds observed after the last fault ends
STEADY_WINDOW = 30.0       # last N seconds: a converged controller must make zero writes
HOT_LOOP_REQS_PER_S = 50   # more controller requests than this in any 1 s bucket = hot loop

FAULT_WEIGHTS = {
    "delete_deployment": 3, "delete_service": 3, "image_drift": 2, "replica_drift": 2,
    "service_port_drift": 2, "selector_drift": 2, "annotate": 2, "spec_replicas": 1,
    "spec_image_good": 1, "spec_image_broken": 1, "spec_port": 1, "squatter": 1, "rogue_writer": 1,
    "api_errors": 2, "watch_break": 1, "crash_leader": 1,
}


def is_broken(image: str | None) -> bool:
    return bool(image) and "broken" in image


@dataclass
class Schedule:
    seed: int
    faults: list[dict]
    watch_delay_max: float
    watch_drop_prob: float
    delete_cr: bool
    quiet_start: float = 0.0
    fault_types: list[str] = field(default_factory=list)


def make_schedule(seed: int) -> Schedule:
    rng = random.Random(seed)
    faults = []
    for _ in range(rng.randint(3, 10)):
        kind = rng.choices(list(FAULT_WEIGHTS), weights=list(FAULT_WEIGHTS.values()))[0]
        f = {"type": kind, "t": round(rng.uniform(10, 60), 3), "dur": 0.0}
        if kind in ("squatter", "rogue_writer"):
            f["dur"] = round(rng.uniform(2, 15), 3)
        if kind == "squatter":
            f["kind"] = rng.choice(["Deployment", "Service"])
        if kind == "rogue_writer":
            f["period"] = round(rng.uniform(0.3, 1.5), 3)
        if kind == "api_errors":
            f["dur"], f["p"] = round(rng.uniform(2, 15), 3), rng.choice([0.3, 0.6, 1.0])
        if kind == "watch_break":
            f["dur"] = round(rng.uniform(1, 10), 3)
        if kind == "crash_leader":
            f["dur"] = round(rng.uniform(3, 40), 3)
        if kind == "spec_replicas":
            f["value"] = rng.randint(0, 5)
        if kind in ("spec_image_good", "spec_image_broken"):
            f["value"] = f"registry.local/model:v{rng.randint(2, 9)}" + ("-broken" if kind.endswith("broken") else "")
        if kind == "spec_port":
            f["value"] = rng.choice([80, 8080, 9000])
        faults.append(f)
    delete_cr = rng.random() < 0.12
    if delete_cr:
        faults.append({"type": "delete_cr", "t": round(max(f["t"] + f["dur"] for f in faults) + rng.uniform(1, 5), 3), "dur": 0.0})
    faults.sort(key=lambda f: f["t"])
    s = Schedule(seed, faults, rng.choice([0.005, 0.1, 0.5, 2.0]), rng.choice([0.0, 0.0, 0.02, 0.1]), delete_cr)
    s.quiet_start = max(f["t"] + f["dur"] for f in faults)
    s.fault_types = sorted({f["type"] for f in faults})
    return s


INITIAL_SPEC = {"image": "registry.local/model:v1", "replicas": 2, "port": 8080, "progressDeadlineSeconds": 20}


def healthy(server: FakeAPIServer, cr_uid: str, expect_deleted: bool, check_status: bool) -> bool:
    """The end state a user would want, judged from the API server's real objects."""
    owned = [o for o in server.objects.values() if controller_owner_uid(o) == cr_uid]
    cr = server.raw(KIND, NS, NAME)
    if expect_deleted:
        return cr is None and not owned
    if cr is None:
        return False
    spec = cr["spec"]
    dep, svc = server.raw("Deployment", NS, NAME), server.raw("Service", NS, NAME)
    if not dep or not svc or controller_owner_uid(dep) != cr_uid or controller_owner_uid(svc) != cr_uid:
        return False
    containers = dep["spec"]["template"]["spec"]["containers"]
    sel = {"app.kubernetes.io/name": NAME}
    if (dep["spec"].get("replicas") != spec["replicas"] or len(containers) != 1
            or containers[0]["ports"][0]["containerPort"] != spec["port"]
            or svc["spec"].get("selector") != sel
            or [(p["port"], p["targetPort"]) for p in svc["spec"]["ports"]] != [(spec["port"], spec["port"])]):
        return False
    image = container_image(dep)
    if rollout_state(dep) != "complete":
        return False  # healthy means the (fake) Deployment controller reports it available
    if is_broken(spec["image"]):
        if is_broken(image):
            return False
    elif image != spec["image"]:
        return False
    if check_status:
        st = cr.get("status") or {}
        if st.get("observedGeneration") != cr["metadata"]["generation"]:
            return False
        ready = condition(cr, "Ready") or {}
        want_ready = "False" if is_broken(spec["image"]) else "True"
        if ready.get("status") != want_ready:
            return False
        if is_broken(spec["image"]) and (condition(cr, "Degraded") or {}).get("reason") != "RolledBack":
            return False
    return True


def run_one(variant: str, sched: Schedule, record_trace: bool = False) -> dict:
    sim = Sim()
    rng = random.Random(sched.seed * 1000 + VARIANTS.index(variant))
    chaos = ChaosConfig(error_windows=[(f["t"], f["t"] + f["dur"], f["p"]) for f in sched.faults
                                       if f["type"] == "api_errors"])
    server = FakeAPIServer(sim, rng, chaos)
    FakeDeploymentController(sim, server)
    cfg = SimConfig(watch_delay_max=sched.watch_delay_max, watch_drop_prob=sched.watch_drop_prob)
    metrics = Metrics()
    user = server.client("user")
    kubectl = server.client("kubectl")
    rogue = server.client("rogue")

    cr = user.create(KIND, NS, {"apiVersion": "portfolio.nikhil.dev/v1alpha1", "kind": KIND,
                                "metadata": {"name": NAME, "namespace": NS}, "spec": dict(INITIAL_SPEC)})
    cr_uid = cr["metadata"]["uid"]
    distractor = kubectl.create("Deployment", NS, {
        "metadata": {"name": f"{NAME}-handmade", "labels": {"app.kubernetes.io/name": NAME}},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "handmade"}},
                 "template": {"metadata": {"labels": {"app": "handmade"}},
                              "spec": {"containers": [{"name": "c", "image": "someone/else:1", "ports": [{"containerPort": 80}]}]}}}})
    def fingerprint(o):
        return o and (o["spec"], o["metadata"].get("labels"), o["metadata"].get("ownerReferences"), o["metadata"]["uid"])
    distractor_fp = fingerprint(distractor)

    replicas: list = []
    if variant == "none":  # one-shot `kubectl apply` of the rendered children, then nothing
        d = Desired.from_resource(cr)
        kubectl.create("Deployment", NS, desired_deployment(d, d.image))
        kubectl.create("Service", NS, desired_service(d))
    elif variant.startswith("v2"):
        for ident in ("ctrl-a", "ctrl-b"):
            replicas.append(V2Replica(sim, server, ident, rng, cfg, metrics,
                                      leader_election=(variant != "v2_no_lease"),
                                      refresh_on_conflict=(variant != "v2_no_refresh")))
    elif variant == "v1":
        replicas.append(PollerReplica(sim, server, "ctrl-a", cfg, metrics, "v1"))
    else:
        for ident in ("ctrl-a", "ctrl-b"):
            replicas.append(PollerReplica(sim, server, ident, cfg, metrics, "naive"))
    for i, r in enumerate(replicas):
        sim.at(0.01 * i, r.start)

    # ---- fault injection
    def safe(fn):
        def wrapped():
            try:
                fn()
            except (ApiError, KeyError, IndexError, TypeError):
                pass  # object missing at injection time: the fault is a no-op
        return wrapped

    def set_container(client, **changes):
        dep = server.raw("Deployment", NS, NAME)
        if dep:
            c = dict(dep["spec"]["template"]["spec"]["containers"][0], **changes)
            client.patch("Deployment", NS, NAME, {"spec": {"template": {"spec": {"containers": [c]}}}})

    def schedule_fault(f):
        t, typ = f["t"], f["type"]
        if typ == "delete_deployment":
            sim.at(t, safe(lambda: kubectl.delete("Deployment", NS, NAME)))
        elif typ == "delete_service":
            sim.at(t, safe(lambda: kubectl.delete("Service", NS, NAME)))
        elif typ == "image_drift":
            sim.at(t, safe(lambda: set_container(kubectl, image="nginx:wrong")))
        elif typ == "replica_drift":
            sim.at(t, safe(lambda: kubectl.patch("Deployment", NS, NAME, {"spec": {"replicas": 9}})))
        elif typ == "service_port_drift":
            sim.at(t, safe(lambda: kubectl.patch("Service", NS, NAME, {"spec": {"ports": [
                {"name": "http", "port": 81, "targetPort": 81, "protocol": "TCP"}]}})))
        elif typ == "selector_drift":
            sim.at(t, safe(lambda: kubectl.patch("Service", NS, NAME, {"spec": {"selector": {
                "app.kubernetes.io/name": None, "wrong": "label"}}})))
        elif typ == "annotate":
            sim.at(t, safe(lambda: kubectl.patch("Deployment", NS, NAME, {"metadata": {"annotations": {"touched": str(t)}}})))
        elif typ == "spec_replicas":
            sim.at(t, safe(lambda: user.patch(KIND, NS, NAME, {"spec": {"replicas": f["value"]}})))
        elif typ in ("spec_image_good", "spec_image_broken"):
            sim.at(t, safe(lambda: user.patch(KIND, NS, NAME, {"spec": {"image": f["value"]}})))
        elif typ == "spec_port":
            sim.at(t, safe(lambda: user.patch(KIND, NS, NAME, {"spec": {"port": f["value"]}})))
        elif typ == "squatter":
            kind = f["kind"]
            held = {}

            def squat():
                try:
                    kubectl.delete(kind, NS, NAME)
                except NotFound:
                    pass
                body = {"metadata": {"name": NAME, "labels": {"team": "someone-else"}}}
                if kind == "Deployment":
                    body["spec"] = {"replicas": 1, "selector": {"matchLabels": {"app": "theirs"}}, "template": {
                        "metadata": {"labels": {"app": "theirs"}},
                        "spec": {"containers": [{"name": "theirs", "image": "someone/else:2", "ports": [{"containerPort": 5000}]}]}}}
                else:
                    body["spec"] = {"selector": {"app": "theirs"}, "ports": [{"name": "x", "port": 5000, "targetPort": 5000}]}
                try:
                    held["uid"] = kubectl.create(kind, NS, body)["metadata"]["uid"]
                except AlreadyExists:
                    pass

            def release():
                if "uid" in held:
                    try:
                        kubectl.delete(kind, NS, NAME, uid=held["uid"])
                    except ApiError:
                        pass
            sim.at(t, squat)
            sim.at(t + f["dur"], release)
        elif typ == "rogue_writer":
            k = 0
            while t + k * f["period"] < f["t"] + f["dur"]:
                tt = t + k * f["period"]
                if k % 2 == 0:
                    sim.at(tt, safe(lambda: rogue.patch("Deployment", NS, NAME, {"spec": {"replicas": 7}})))
                else:
                    sim.at(tt, safe(lambda: set_container(rogue, image="rogue/model:latest")))
                k += 1
        elif typ == "watch_break":
            def brk():
                for r in replicas:
                    if isinstance(r, V2Replica) and r.alive:
                        r.informer.break_watch(sim.now + f["dur"])
            sim.at(t, brk)
        elif typ == "crash_leader":
            def crash():
                live = [r for r in replicas if r.alive]
                if not live:
                    return
                victim = next((r for r in live if r.leading), live[0])
                victim.crash()
                sim.after(f["dur"], victim.start)
            sim.at(t, crash)
        elif typ == "delete_cr":
            sim.at(t, safe(lambda: user.delete(KIND, NS, NAME)))

    for f in sched.faults:
        schedule_fault(f)

    # ---- online invariant observers
    check_status = variant.startswith("v2")
    obs = {"max_owned_per_kind": 0, "converged_since": None, "max_leaders": 0,
           "last_t": WARMUP, "last_ok": False, "unhealthy_s": 0.0, "last_obj_ok": False, "unhealthy_obj_s": 0.0}
    delete_t = next((f["t"] for f in sched.faults if f["type"] == "delete_cr"), None)
    trace: list[tuple[float, bool]] = []

    def observe():
        per_kind = Counter(o["kind"] for o in server.objects.values() if controller_owner_uid(o) == cr_uid)
        obs["max_owned_per_kind"] = max([obs["max_owned_per_kind"], *per_kind.values()])
        obs["max_leaders"] = max(obs["max_leaders"], sum(1 for r in replicas if isinstance(r, V2Replica)
                                                         and r.alive and r.elector and r.elector.is_leader))
        if sim.now >= WARMUP:
            ok_now = healthy(server, cr_uid, delete_t is not None and sim.now >= delete_t, check_status)
            obj_ok = ok_now if not check_status else healthy(server, cr_uid, delete_t is not None and sim.now >= delete_t, False)
            if not obs["last_ok"]:
                obs["unhealthy_s"] += sim.now - obs["last_t"]
            if not obs["last_obj_ok"]:
                obs["unhealthy_obj_s"] += sim.now - obs["last_t"]
            obs["last_t"], obs["last_ok"], obs["last_obj_ok"] = sim.now, ok_now, obj_ok
        if sim.now >= sched.quiet_start:
            ok = ok_now
            if ok and obs["converged_since"] is None:
                obs["converged_since"] = sim.now
            elif not ok:
                obs["converged_since"] = None
            if record_trace:
                trace.append((sim.now, ok))

    sim.observers.append(observe)
    horizon = sched.quiet_start + QUIET_TAIL
    sim.run_until(sched.quiet_start)
    observe()
    sim.run_until(horizon)
    observe()

    # ---- audit-log invariants
    ctrl = [a for a in server.audit if a.actor.startswith(CONTROLLER_PREFIX)]
    ok_writes = [a for a in ctrl if a.verb in WRITE_VERBS and a.code == 200 and a.kind != "Lease"]
    unowned_writes = [a for a in ok_writes if a.target_existed and a.kind in ("Deployment", "Service")
                      and a.target_owner != cr_uid and a.verb != "create"]
    split_brain = [a for a in ok_writes if a.lease_holder_ok is False]
    buckets = Counter(int(a.t) for a in ctrl)
    steady = [a for a in ok_writes if a.t >= horizon - STEADY_WINDOW]
    converged = obs["converged_since"] is not None
    conv_t = max(0.0, obs["converged_since"] - sched.quiet_start) if converged else None
    steps = (sum(1 for t, _ in metrics.reconciles if sched.quiet_start <= t <= obs["converged_since"])
             if converged else None)
    distractor_now = server.raw("Deployment", NS, f"{NAME}-handmade")
    distractor_writes = sum(1 for a in ok_writes if a.name == f"{NAME}-handmade")
    out = {
        "seed": sched.seed, "variant": variant, "converged": converged,
        "convergence_s": round(conv_t, 4) if conv_t is not None else None,
        "convergence_reconciles": steps,
        "unhealthy_s": round(obs["unhealthy_s"], 3),
        "unhealthy_objects_only_s": round(obs["unhealthy_obj_s"], 3),
        "requests": len(ctrl), "writes": len(ok_writes),
        "conflicts_409": sum(1 for a in ctrl if a.code == 409),
        "errors_503": sum(1 for a in ctrl if a.code == 503),
        "reconcile_errors": metrics.reconcile_errors,
        "max_backoff_s": round(metrics.max_backoff, 3),
        "max_requests_per_s": max(buckets.values(), default=0),
        "steady_requests_per_s": round(sum(1 for a in ctrl if a.t >= horizon - STEADY_WINDOW) / STEADY_WINDOW, 3),
        "steady_writes": len(steady),
        "unowned_writes": len(unowned_writes),
        "split_brain_writes": len(split_brain),
        "max_simultaneous_leaders": obs["max_leaders"],
        "max_owned_per_kind": obs["max_owned_per_kind"],
        "distractor_untouched": bool(distractor_writes == 0 and fingerprint(distractor_now) == distractor_fp),
        "sim_events": sim.events_run,
    }
    out["invariants_ok"] = (out["unowned_writes"] == 0 and out["split_brain_writes"] == 0
                            and out["max_owned_per_kind"] <= 1 and out["distractor_untouched"]
                            and out["max_requests_per_s"] <= HOT_LOOP_REQS_PER_S
                            and (not converged or out["steady_writes"] == 0))
    if record_trace:
        out["trace"] = trace
        out["server"] = server
    return out


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return round(v[lo] + (v[hi] - v[lo]) * (k - lo), 3)


def summarize(rows: list[dict]) -> dict:
    conv = [r["convergence_s"] for r in rows if r["converged"]]
    steps = [r["convergence_reconciles"] for r in rows if r["converged"]]
    n = len(rows)
    return {
        "runs": n,
        "converged": sum(r["converged"] for r in rows),
        "invariants_ok": sum(r["invariants_ok"] for r in rows),
        "converged_and_invariants_ok": sum(r["converged"] and r["invariants_ok"] for r in rows),
        "convergence_s": {"p50": pct(conv, .5), "p90": pct(conv, .9), "p99": pct(conv, .99),
                          "max": max(conv) if conv else None},
        "unhealthy_s": {"p50": pct([r["unhealthy_s"] for r in rows], .5), "p90": pct([r["unhealthy_s"] for r in rows], .9),
                        "max": max(r["unhealthy_s"] for r in rows)},
        "unhealthy_objects_only_s": {"p50": pct([r["unhealthy_objects_only_s"] for r in rows], .5),
                                     "p90": pct([r["unhealthy_objects_only_s"] for r in rows], .9),
                                     "max": max(r["unhealthy_objects_only_s"] for r in rows)},
        "convergence_reconciles": {"p50": pct(steps, .5), "p90": pct(steps, .9), "max": max(steps) if steps else None},
        "runs_with_unowned_writes": sum(r["unowned_writes"] > 0 for r in rows),
        "total_unowned_writes": sum(r["unowned_writes"] for r in rows),
        "runs_with_split_brain_writes": sum(r["split_brain_writes"] > 0 for r in rows),
        "runs_with_duplicate_owned_objects": sum(r["max_owned_per_kind"] > 1 for r in rows),
        "runs_with_hot_loop": sum(r["max_requests_per_s"] > HOT_LOOP_REQS_PER_S for r in rows),
        "runs_with_steady_state_writes": sum(r["converged"] and r["steady_writes"] > 0 for r in rows),
        "max_requests_per_s": max(r["max_requests_per_s"] for r in rows),
        "requests_per_run": {"median": statistics.median(r["requests"] for r in rows),
                             "max": max(r["requests"] for r in rows)},
        "writes_per_run_median": statistics.median(r["writes"] for r in rows),
        "steady_requests_per_s_median": statistics.median(r["steady_requests_per_s"] for r in rows),
        "conflicts_409_total": sum(r["conflicts_409"] for r in rows),
        "errors_503_total": sum(r["errors_503"] for r in rows),
        "max_simultaneous_leaders": max(r["max_simultaneous_leaders"] for r in rows),
    }


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seeds", type=int, default=300)
    ap.add_argument("--first-seed", type=int, default=0)
    ap.add_argument("--variants", default=",".join(VARIANTS))
    ap.add_argument("--output", default="results-chaos.json")
    args = ap.parse_args(argv)
    variants = args.variants.split(",")
    seeds = list(range(args.first_seed, args.first_seed + args.seeds))
    scheds = [make_schedule(s) for s in seeds]
    t0, c0 = time.perf_counter(), time.process_time()
    rows: dict[str, list[dict]] = {v: [] for v in variants}
    for v in variants:
        for s in scheds:
            rows[v].append(run_one(v, s))
        print(f"{v:12s} done ({time.perf_counter() - t0:.1f}s wall)", file=sys.stderr)
    wall, cpu = time.perf_counter() - t0, time.process_time() - c0
    by_family = {}
    for fam, pred in {"with_broken_image": lambda s: "spec_image_broken" in s.fault_types,
                      "with_crash_leader": lambda s: "crash_leader" in s.fault_types,
                      "with_api_errors": lambda s: "api_errors" in s.fault_types,
                      "with_squatter": lambda s: "squatter" in s.fault_types,
                      "with_cr_deletion": lambda s: s.delete_cr,
                      "with_silent_watch_drops": lambda s: s.watch_drop_prob > 0,
                      "without_silent_watch_drops": lambda s: s.watch_drop_prob == 0}.items():
        idx = [i for i, s in enumerate(scheds) if pred(s)]
        by_family[fam] = {"schedules": len(idx), **{v: {
            "converged": sum(rows[v][i]["converged"] for i in idx),
            "convergence_s_p50": pct([rows[v][i]["convergence_s"] for i in idx if rows[v][i]["converged"]], .5),
            "convergence_s_max": max([rows[v][i]["convergence_s"] for i in idx if rows[v][i]["converged"]], default=None),
            "unhealthy_s_p50": pct([rows[v][i]["unhealthy_s"] for i in idx], .5),
            "unhealthy_s_max": max([rows[v][i]["unhealthy_s"] for i in idx], default=None),
            "unhealthy_objects_only_s_p50": pct([rows[v][i]["unhealthy_objects_only_s"] for i in idx], .5),
        } for v in variants}}
    result = {
        "scope": ("OFFLINE SIMULATION on the fake API server in offline_api.py. Times are simulated seconds "
                  "under assumed latencies; they are not Kubernetes measurements. Separate from results-live.json."),
        "command": "python3 chaos.py " + " ".join(argv if argv is not None else sys.argv[1:]),
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "wall_clock_s": round(wall, 1), "cpu_s": round(cpu, 1),
                        "load_average_at_end": [round(x, 1) for x in __import__("os").getloadavg()], "date": time.strftime("%Y-%m-%d")},
        "seeds": [seeds[0], seeds[-1]],
        "model_assumptions": {
            "request_latency_s": ChaosConfig().latency, "rollout_delay_s": ChaosConfig().rollout_delay,
            "gc_delay_s": ChaosConfig().gc_delay, "progress_deadline_s": INITIAL_SPEC["progressDeadlineSeconds"],
            "lease": {"duration": 15, "renew_deadline": 10, "retry_period": 2}, "relist_period_s": 30,
            "backoff": {"base": 0.005, "cap": 30, "jitter": "equal (50-100%)", "bucket_qps": 10, "burst": 100},
            "poll_interval_v1_naive_s": 0.5, "naive_retry_s": 0.01, "quiet_tail_s": QUIET_TAIL,
            "hot_loop_threshold_requests_per_s": HOT_LOOP_REQS_PER_S, "steady_window_s": STEADY_WINDOW,
            "fault_weights": FAULT_WEIGHTS, "faults_per_schedule": "3-10 in t=[10,60] s, +12% chance of CR deletion at end",
            "watch_delay_max_choices_s": [0.005, 0.1, 0.5, 2.0], "watch_silent_drop_choices": [0.0, 0.0, 0.02, 0.1],
        },
        "definitions": {
            "converged": "from the end of the last fault, the API objects reach and STAY in the healthy state until "
                         "the end of the run: owned Deployment+Service match spec, fake Deployment controller reports "
                         "it available, image == spec.image (or, if spec.image is broken, a non-broken image), and for "
                         "v2 variants status.observedGeneration == generation with the right Ready/Degraded condition. "
                         "If the schedule deletes the CR: CR and all owned objects gone.",
            "convergence_s": "simulated seconds from the end of the last fault window to stable convergence",
            "convergence_reconciles": "reconcile calls (all replicas) in that interval",
            "unhealthy_s": "total simulated seconds from t=10 s to the end of the run during which the healthy check "
                           "failed (captures outages DURING fault windows, e.g. while a crashed v1 is down)",
            "unhealthy_objects_only_s": "same, but never checks the CR status (v1/naive/none write no status, so this "
                                        "is the like-for-like comparison across variants)",
            "invariants_ok": "no successful controller write to an object not controlled by this CR; no write by a "
                             "non-Lease-holder (v2); <=1 owned object per kind at every instant; hand-made look-alike "
                             f"Deployment untouched; <= {HOT_LOOP_REQS_PER_S} controller requests in every 1 s bucket; "
                             f"zero controller writes in the last {STEADY_WINDOW:.0f} s once converged",
        },
        "variants": {
            "none": "children applied once at t=0 (kubectl apply), no controller",
            "naive": "2 replicas, no leader election, no ownership check, blind patches, 10 ms retry, 0.5 s polling",
            "v1": "exact v1 reconcile (v1/controller_v1.py) polled every 0.5 s by one replica - the code measured on k3s",
            "v2": "2 replicas with Lease election, informer cache + watches, rate-limited queue, finalizer, status, rollback",
            "v2_no_lease": "ablation: v2 with both replicas active (relies only on resourceVersion conflicts)",
            "v2_no_refresh": "ablation: v2 without the targeted cache re-read after a 409 (the first version I wrote)",
        },
        "summary": {v: summarize(rows[v]) for v in variants},
        "by_fault_family": by_family,
        "runs": rows,
    }
    Path(args.output).write_text(json.dumps(result, indent=1) + "\n")
    print(json.dumps({"wall_clock_s": round(wall, 1), "cpu_s": round(cpu, 1), "summary": result["summary"],
                      "by_fault_family": by_family}, indent=1))


if __name__ == "__main__":
    main()
