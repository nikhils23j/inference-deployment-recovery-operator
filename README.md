# Inference Deployment Recovery Operator

I kept deleting my own Deployment by accident while messing around with a local
k3s cluster, so I built the thing that puts it back.

It's a small Kubernetes operator in Python. You describe an `InferenceService`
(image, replicas, port), and it keeps a matching Deployment and Service alive: it recreates them
if they're deleted, undoes edits to the fields it manages, refuses to touch
objects it doesn't own, and rolls back an image that never becomes available.
"Inference" is aspirational. The sample image is nginx, and this project is about
keeping the objects right, not about serving models.

It's a personal project. I built it with AI pair-programming help (I'd describe a
piece, we'd write it together, and I'd break it and read the traces). Nobody uses
it in production.

## Two versions, two kinds of evidence

| | v1 (Oct 2026) | v2 (this version) |
|---|---|---|
| loop | poll every 0.5 s, direct GETs | watches → informer cache → rate-limited work queue |
| writes | blind JSON patches | full updates with a `resourceVersion` precondition (409 on stale reads) |
| ownership | refuses unowned objects | same, plus reports it in status (`Degraded/ChildNotOwned`) |
| replicas | one | two, with Lease leader election |
| status | none | `observedGeneration`, `Ready`/`Progressing`/`Degraded` conditions |
| deletion | relies on ownerReference GC | finalizer that deletes only owned children (uid precondition) |
| bad images | keeps applying them forever | rolls back to last good image after `ProgressDeadlineExceeded` |
| evidence | **real k3s run** (`results-live.json`) | **offline simulation** (`results-chaos.json`, `results-offline.json`) |

The v1 code is frozen in `v1/` because it's what produced the live numbers.

## How it works

```
             watch events (may be delayed / dropped)          every 30 s: full relist
 API server ───────────────────────────────────────┐        ┌──────────────────────┐
     ▲                                             ▼        ▼                      │
     │                                      ┌──────────────────┐                   │
     │                                      │  informer cache  │───────────────────┘
     │                                      └────────┬─────────┘
     │                          map event → owning CR key (ns/name)
     │                                               ▼
     │                         ┌──────────────────────────────────────────┐
     │                         │ work queue: dedup, one-at-a-time per key, │
     │                         │ exp. backoff + jitter, token bucket       │
     │                         └────────────────────┬─────────────────────┘
     │                                              ▼  (only if I hold the Lease)
     │        ┌────────────────────────────────────────────────────────────────┐
     └────────│ reconcile(key):  read CR + children from cache                 │
   writes w/  │   terminating? → delete owned children, drop finalizer         │
   resource-  │   missing child → create      not ours → don't touch, report   │
   Version    │   drifted → update managed fields only                         │
   precond.   │   rollout failed? → remember image, roll back to last good     │
              │   status changed? → write /status (else: write nothing)        │
              └────────────────────────────────────────────────────────────────┘
                 error → requeue with backoff     409 → re-read that key, requeue
```

A few decisions and why:

- **Level-triggered.** Reconcile never looks at *what* event happened, only at
  the current state. A dropped event costs time, not correctness, because the
  periodic relist re-enqueues everything.
- **Optimistic concurrency instead of locks.** Every update carries the
  `resourceVersion` I read. If anyone wrote in between, I get a 409 and try
  again, instead of silently overwriting their change.
- **Only manage my fields.** Drift repair overwrites replicas, image, container
  port, progress deadline, the Service selector and ports. Annotations, env vars
  and anything else someone adds survive. (This isn't real server-side apply. It's
  a hand-written version of the same idea.)
- **Lease leader election** like client-go's: expiry is judged by when *I* last
  saw the Lease change, not by the other replica's clock. The leader stops writing
  after 10 s without a successful renew, before the 15 s lease can expire.
- **Status writes only when something changed.** `lastTransitionTime` only moves
  when a condition flips. Otherwise every status write would trigger an event,
  which triggers a reconcile, which writes status again.
- **Rollback rule:** if the Deployment for the *current* generation reports
  `ProgressDeadlineExceeded` while running `spec.image`, I record it as
  `failedImage` and go back to `lastGoodImage`. Pushing a different image clears
  it. A drifted image that fails is never blamed on the spec.

## The chaos harness

`offline_api.py` is a fake API server on a simulated clock. It has resourceVersions,
generations, a status subresource, finalizers, ownerReference GC, 409s, injected
503s, watch streams with delays, breaks and silent drops, and a fake Deployment
controller (images tagged `-broken` never become available). `chaos.py` generates a
random fault schedule per seed and runs every controller variant against it:

- deleting the Deployment or Service
- drift in image, replicas, port or selector
- a harmless annotation (bumps resourceVersion)
- user spec changes, including broken images
- a **squatter** (someone deletes my Deployment and creates their own with the same name)
- a **rogue writer** fighting the controller
- windows of 503s (30–100% of requests)
- watch breaks
- **killing the leader**
- deleting the CR

An invariant checker reads the audit log and the object store after every event:

- converges to the healthy state after the last fault and stays there
- never successfully writes to an object it doesn't control
- never writes while not holding the Lease
- at most one owned object per kind
- a hand-made look-alike Deployment is never touched
- at most 50 controller requests in any 1 s bucket (hot-loop check)
- zero writes in the final 30 s once converged

**Variants:**

- `none`: apply the children once, no controller
- `naive`: two pollers, no ownership check, blind patches, 10 ms retry, no leader election
- `v1`: the exact old code, polling
- `v2`
- two ablations of v2: without the Lease, and without the 409 re-read

## What surprised us / bugs the chaos runs found

1. **One dropped event → 30 s of 409s.** My first v2 read everything from the
   cache. When the watch event for *my own* status write got dropped, the cache
   kept an old resourceVersion, and every status write 409'd and backed off until the
   30 s relist. Fix: after a 409 (or 404/AlreadyExists), re-read that one key from
   the API. The `v2_no_refresh` ablation keeps the old behaviour: across the same
   300 schedules it hit **7,214** 409s vs **1,276** with the fix, and its worst-case
   convergence was 67.3 vs 31.7 simulated seconds.
2. **Forgetting what "good" was.** Seed 72: the "rollout finished" event was
   dropped, then the leader crashed, then the user pushed a broken image. The standby
   took over and went straight to the broken image. It had never recorded v1 as
   good, so it had nothing to roll back to and stayed broken. 2 of 300 ablation runs
   never converged (`results-chaos-before-lastgood-fix.json`). Fix: remember which
   image I last applied (`status.appliedImage`) and mark it good *before* changing
   it, if it's fully rolled out. There's a regression test for this exact sequence.
3. **Two replicas without a Lease ping-pong the status.** In `v2_no_lease` seed 188
   one replica's cache had missed "rollout complete", so it thought Ready=False while
   the other thought True. They overwrote each other **117 times in one second**.
   Every write *succeeded*, so resourceVersion checks don't help here. That one run is
   why leader election exists.
4. **The boring poller is hard to beat on speed.** v1 does 6 fresh GETs a second
   and never trusts a cache, so lossy watches don't hurt it. On median "time spent
   unhealthy", v1 (14.8 s) is slightly better than v2 (15.7 s objects-only). v2's
   wins are elsewhere: rollback, crash tail, request count, and never touching
   things it doesn't own.
5. Smaller ones: the finalizer looped on 404 after the CR was already gone (treat
   NotFound as done). My first invariant check said everything failed because the fake
   Deployment controller writes status on the look-alike object (compare spec, not
   resourceVersion).

## Results

### Offline simulation (new, `results-chaos.json`)

Command `python3 chaos.py --seeds 300`, seeds 0–299, every variant on the same 300
schedules. It took 52 s wall / 51.5 s CPU on an M2 MacBook (Python 3.10.8). All times are **simulated
seconds** on the fake API server with assumed latencies (1–5 ms per request,
1–4 s rollouts, 20 s progress deadline, 15 s Lease). They aren't Kubernetes
latencies.

| | none | naive | v1 | **v2** | v2 no Lease | v2 no 409 re-read |
|---|---|---|---|---|---|---|
| converged & stayed healthy (of 300) | 41 | 258 | 258 | **300** | 300 | 300 |
| runs with all invariants held | 300 | 166 | 300 | **300** | 299 | 300 |
| runs that wrote to an object they didn't own | 0 | 63 | 0 | **0** | 0 | 0 |
| runs over the hot-loop limit (max req/s) | 0 | 92 (196) | 0 (10) | **0 (26)** | 1 (117) | 0 (31) |
| convergence after last fault p50 / p90 / max (sim s) | — | 0.5 / 3.3 / 4.2 | 0.5 / 3.2 / 4.3 | **0.73 / 16.3 / 31.7** | 0.38 / 10.8 / 28.4 | 0.96 / 18.6 / 67.3 |
| time unhealthy from t=10 s, objects only, p50 / max | 194.5 / 233.2 | 10.5 / 196.9 | 14.8 / 196.9 | **15.7 / 75.9** | 20.9 / 74.0 | 16.3 / 93.7 |
| controller API requests per run (median) | 0 | 2,470 | 1,197 | **359** | 105 | 364 |
| steady-state requests/s (median) | 0 | 11.8 | 5.9 | **1.6** | 0.2 | 1.6 |
| 409 conflicts (total) | 0 | 0 | 0 | **1,276** | 3,670 | 7,214 |
| max replicas that believed they led at once | — | — | — | **1** | — | 1 |

How to read it:

- Every v1 and naive failure (42 each) is a schedule whose last spec change was a broken
  image. Neither can roll back. In the 51 schedules that pushed a broken image at
  some point, v2 converged in **51/51** and v1 in **9/51**.
- In the 54 schedules that kill the leader, v2's worst total unhealthy time in a run was
  **75.9 s** vs **188.8 s** for v1, which is down until restarted. Medians (objects only): 21.5 vs 29.7 s.
- v2's slower p90/max convergence comes mostly from waiting out the 20 s progress
  deadline before rolling back, the 15 s Lease takeover, and the 30 s relist after
  silently dropped events.
- v2's request count is mostly Lease renewals (two replicas, every ~2 s). The
  no-Lease ablation shows the reconcile path itself needs about 105 requests per run.
- "naive converges" partly because it adopts squatters: it rewrites someone else's
  object until it looks like ours.

`python3 demo_offline.py` also replays the live evaluator's 30 deletion and drift faults in
the simulator (`results-offline.json`). Both versions repaired 30/30. Median repair
was 0.265 sim-s for v1 and 0.018 sim-s for v2, and controller requests were 6.0 vs 1.9 per sim-second. The gap is mostly
v1's 0.5 s poll interval, so it says nothing about real latency.

### Live k3s (old, v1 code, `results-live.json`)

Measured on 30 October 2026 with the **v1 controller** (now in `v1/`) on a
one-node Lima/k3s cluster on macOS arm64 (Python 3.10.8, kubernetes client 35.0.0).
I haven't re-run this since: no cluster right now. v2 has **not** been measured
live.

- 30/30 injected deletion and drift faults repaired, max **0.869 s**, median 0.48 s
- at most 2 owned objects (one Deployment, one Service) at any check; no controller errors
- with the controller stopped, a deleted Deployment was still gone after 3 s
- this timing is API-object convergence, not Pod readiness

## Break it yourself

Offline (Python 3.10+, stdlib only):

```sh
python3 -m unittest -v test_controller test_runtime   # 27 tests, ~10 s
python3 demo_offline.py                               # narrated rollback + crash demo
python3 chaos.py --seeds 300                          # ~1 min on an M2
python3 chaos.py --seeds 20 --variants v2,v1 --output my-run.json
```

To look at one run, `chaos.run_one("v2", chaos.make_schedule(72), record_trace=True)["server"].audit`
gives every request with time, actor, verb and status code.

Live, on k3s, kind or minikube:

```sh
python3 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
kubectl apply -f crd.yaml
kubectl wait --for=condition=Established --timeout=60s crd/inferenceservices.portfolio.nikhil.dev
kubectl apply -f sample.yaml
.venv/bin/python k8s_adapter.py --namespace default        # start a second copy to see the Lease
kubectl get isvc,deploy,svc,lease
kubectl delete deploy demo-inference                       # comes back
kubectl patch isvc demo-inference --type merge -p '{"spec":{"image":"nginx:does-not-exist-broken"}}'
#   ...after progressDeadlineSeconds (60 s in sample.yaml) it should roll back to nginx:1.27-alpine
```

To re-run the 30-fault live evaluation, stop the operator first:

```sh
.venv/bin/python evaluate_live.py --controller v2   # -> results-live-v2-rerun.json
.venv/bin/python evaluate_live.py --controller v1   # the original loop, for comparison
```

It refuses to overwrite `results-live.json`. The operator runs outside the cluster
from your kubeconfig. There's no in-cluster Deployment or RBAC manifest yet.

## What this isn't

- **Not measured live in its v2 form.** The live adapter (`k8s_adapter.py`)
  is unit-tested against a mocked client, and every method name it uses exists in
  kubernetes-client 35.0.0. It has never talked to a real API server. Expect
  something to break the first time.
- **The simulator is my model of Kubernetes, not Kubernetes.** A whole
  reconcile runs atomically at one simulated instant, there are no admission
  webhooks or field managers, and "available" is decided by a fake Deployment
  controller. Silent watch drops are an adversarial test. Real watches fail
  loudly and get relisted.
- **No duplicates is mostly structural.** Children have deterministic names,
  so a duplicate would need a different code path. The check is a regression guard.
- **The finalizer is mostly redundant.** ownerReference GC already deletes
  children. I added it to learn the deletion flow, and it only deletes objects it owns.
- **Rollback is a progress-deadline guard, not a canary.** There's no traffic
  splitting and no metrics analysis. "Available" means the Deployment controller says so.
- One namespace, one worker, no metrics endpoint, no webhook validation beyond the
  CRD schema, no security hardening.

## Files

| file | what |
|---|---|
| `controller.py` | desired state, drift checks, `Reconciler` (shared by sim and live) |
| `workqueue.py` / `leader.py` | rate-limited queue / Lease elector |
| `offline_api.py` / `simulation.py` | fake API server, informer, sim replicas |
| `chaos.py` | schedules, invariants, metrics → `results-chaos.json` |
| `baselines.py` | naive controller + adapter for running v1 in the sim |
| `k8s_adapter.py` / `evaluate_live.py` | live runtime / live 30-fault evaluator |
| `v1/` | frozen v1 code and v1 offline result (imports refer to the old module layout) |
