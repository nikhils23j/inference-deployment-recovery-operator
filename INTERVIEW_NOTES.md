# Interview notes: Inference Deployment Recovery Operator

## 1. What it is

A small Kubernetes operator in Python. You declare an `InferenceService` (image,
replicas, port), and it keeps a matching Deployment and Service in the right shape:
it recreates them, undoes drift, refuses to touch objects it doesn't own, and rolls
back images that never become available. It's a personal project. v1 was tested on a real
local k3s cluster; v2 was tested with a simulated API server and a randomized chaos harness.

## 2. How it works: architecture, decisions, reasoning

**Pipeline (v2):** watches → informer cache → event mapped to the owning CR key →
rate-limited work queue → one worker runs `reconcile(key)`. The worker only runs
while this replica holds the Lease.

| decision | why |
|---|---|
| Level-triggered reconcile (look at state, ignore the event type) | Events can be delayed or lost; recomputing from current state makes a lost event cost time, not correctness. A 30 s relist re-enqueues everything. |
| Deterministic child names (same as the CR) | Makes "create twice" impossible by construction; a second create gets 409 AlreadyExists. |
| Controller ownerReference check before any write | Never adopt someone else's object that happens to share the name. Report it as `Degraded/ChildNotOwned` instead. |
| Full update with `resourceVersion` precondition | If someone wrote between my read and my write, I get 409 and retry, instead of blindly overwriting. |
| Update only managed fields | Other people's annotations, env vars, etc. survive drift repair. (A hand-rolled version of the server-side apply idea, not SSA.) |
| On 409/404/AlreadyExists, re-read that key from the API | Found by chaos tests: a dropped event for my own write left the cache stale until the next relist. |
| Work queue like client-go's | Dedup (10 events → 1 reconcile), never the same key in parallel, per-key exponential backoff (5 ms base, 30 s cap, 50–100% jitter) plus a 10 qps / 100 burst token bucket. |
| Lease leader election (15 s lease, 10 s renew deadline, 2 s retry) | Two replicas for availability but one writer. Expiry is judged by *observed time* (when I last saw the Lease change), so clock skew between replicas doesn't matter. The leader stops writing before the lease could expire. |
| Status subresource, `observedGeneration`, conditions | Users and tools can tell whether the controller has seen the latest spec. `lastTransitionTime` only changes on a flip, so unchanged status isn't rewritten (otherwise it hot-loops). |
| Finalizer | Explicit cleanup of owned children with a uid precondition. Honestly mostly redundant with ownerReference GC. |
| Rollback on `ProgressDeadlineExceeded` | Uses the Deployment controller's own signal. Records `failedImage`, serves `lastGoodImage`. A different `spec.image` clears it. Only blames the spec if the failing Deployment generation was actually running `spec.image`. |
| Record "last good" from `appliedImage` *before* changing the Deployment | Found by chaos tests (seed 72): otherwise a missed "rollout complete" moment means nothing to roll back to. |

**Test harness:** `offline_api.py` is a fake API server on a simulated clock. It models
resourceVersion/generation, the status subresource, finalizers, GC, 409s, injected 503s,
watch streams that can be delayed, broken or silently lossy, and a fake Deployment
controller. `chaos.py` runs 300 seeded schedules against 6 variants and checks
invariants from an audit log of every request.

## 3. Key results (only measured numbers)

**Offline simulation, new** (`results-chaos.json`; 300 seeds; `python3 chaos.py --seeds 300`; 52 s on an M2).
Times are *simulated* seconds under assumed latencies.

- v2 converged and stayed healthy in **300/300** schedules with all invariants held:
  - 0 writes to unowned objects
  - never more than 1 replica believing it led
  - 0 hot-loop runs (max 26 req/s)
  - 0 writes in the final steady window
- Old v1 code in the same simulator: 258/300. All 42 failures were schedules ending on a broken image.
  - In the 51 broken-image schedules: v2 **51/51**, v1 **9/51**.
- Naive baseline (no ownership check, no backoff, no Lease):
  - wrote to someone else's object in **63/300** runs
  - exceeded the hot-loop limit in **92/300** (up to 196 req/s)
- Leader-crash schedules (54): worst total unhealthy time v2 **75.9 s** vs v1 **188.8 s**.
- Requests per run (median): v2 **359**, v1 1,197, naive 2,470. Steady state: 1.6 vs 5.9 vs 11.8 req/s.
- Ablations:
  - Without the 409 re-read, v2 hit **7,214** conflicts vs **1,276** with it, and its worst-case convergence was 67.3 vs 31.7 sim-s.
  - Without the Lease, one run ping-ponged status writes at **117/s**.
- Where v2 is *not* better: median time unhealthy, objects only, v2 15.7 s vs v1 14.8 s. A 0.5 s poller with fresh reads is hard to beat when watches are lossy.

**Live k3s, old** (`results-live.json`; 30 Oct 2026; **v1 code**; one-node Lima/k3s on an M2):

- 30/30 injected deletion and drift faults repaired, max **0.869 s**, median 0.48 s
- at most 2 owned objects; no controller errors
- with the controller stopped, a deleted Deployment stayed gone (checked after 3 s)

**What these do NOT show:**

- v2 has never run against a real API server. Its live adapter is only unit-tested with mocks.
- Simulated seconds aren't real seconds. Real latency, etcd and network behaviour aren't modelled.
- Live timing is API-object convergence, not Pod readiness or model serving.
- Single namespace and single CR in every test. No scale testing, no real inference workload.
- "No duplicates" is mostly guaranteed by deterministic naming, not by clever logic.

## 4. Two-minute answer (STAR)

**Situation.**
While learning Kubernetes on a local k3s cluster, I kept breaking my own setup:
deleting a Deployment by accident, or editing something by hand and forgetting. Plain
manifests don't fix themselves, so I wanted to understand how operators actually keep
things in the declared state. It was a personal project, and I built it with AI
pair-programming help: I'd design a piece, we'd write it together, and I'd break it.

**Task.**
The first goal was simple: a Python operator for a small custom resource that recreates and repairs a
Deployment and Service, without ever taking over objects it doesn't own. Then I wanted to make it behave
like a real controller under bad conditions: conflicting writers, API errors, lost
events, a crashing replica, and a bad image push.

**Action.**
Version one was a polling loop. I tested it on k3s with 30 injected deletions and
drift faults, and it repaired all 30 within 0.87 seconds. For version two I rebuilt it the
way client-go controllers work: watches into a cache, a rate-limited work queue with
backoff, writes guarded by resourceVersion so stale reads get a 409 instead of
clobbering, Lease leader election so two replicas don't both write, a status
subresource with conditions, a finalizer, and rollback when an image misses its progress
deadline. I didn't have a cluster any more, so I wrote a fake API server on a simulated
clock plus a chaos harness that runs 300 random fault schedules and checks invariants
from an audit log. It found real bugs. A single dropped watch event for my own status
write caused 30 seconds of conflicts. A crash at the wrong moment made the controller
forget which image was good, so it couldn't roll back.

**Result.**
After the fixes, v2 converged in all 300 simulated schedules with no invariant
violations: it never wrote to an object it didn't own, and never had two leaders. The
old v1 code failed the 42 schedules that ended on a broken image. A naive version
wrote to someone else's object in 63 runs. v2 also used about a third of v1's API requests.
I'm careful about the limits: the v2 numbers are from my simulator, not a real
cluster, and on median recovery time the simple poller was actually slightly faster. The
main thing I learned is that the hard part of a controller is all the ways reads go
stale.

## 5. Likely follow-ups

1. **Why not use kopf or controller-runtime?**
   I wanted to see the moving parts (queue, cache, leases, conflicts) myself. For anything
   real I'd use controller-runtime or kopf, and I'd expect them to handle the edge cases I hit better.
2. **How do you know the simulator is realistic?**
   I don't fully. It encodes my understanding of Kubernetes semantics: resourceVersion
   preconditions, the status subresource, generation, GC. It runs a whole reconcile
   atomically and has no admission or field managers. That's why I label the numbers as
   simulated and keep them separate from the k3s run. Running v2 live is the next step.
3. **What happens if the leader pauses for 20 s (GC pause, VM freeze)?**
   That's the classic lease problem. It could wake up and write once believing it
   still leads. resourceVersion preconditions limit the damage, since a stale write gets a 409.
   But I don't have fencing tokens, and my sim only models crashes, not pauses.
4. **Why is v2's p90 convergence slower than v1's?**
   It deliberately waits out the 20 s progress deadline before rolling back, Lease takeover
   takes about 15 s, and silently dropped events wait up to 30 s for the relist. v1 polls every 0.5 s
   with fresh reads, so lossy watches don't affect it, but it can't roll back.
5. **Why full updates instead of server-side apply?**
   Simplicity. A precondition gives me a clean 409 to reason about. SSA with a field
   manager would be the better production choice: it handles shared ownership of fields properly.
6. **What does the finalizer buy you?**
   Honestly not much here. ownerReference GC already deletes children. It taught me
   the deletion flow (deletionTimestamp, finalizer removal, the 404 edge case). It only deletes
   objects whose uid it owns.
7. **How would you scale this to many CRs?**
   More workers (the queue already guarantees one key at a time), sharding or
   namespace scoping, and watching with label selectors so the cache stays small. I haven't tested any of that.
8. **What's the biggest thing you'd fix next?**
   Run v2 on a real cluster with `evaluate_live.py --controller v2`, then add an in-cluster
   Deployment with RBAC, and measure Pod readiness rather than just object state.

## 6. Things I should be able to explain cold

- [ ] Level-triggered vs edge-triggered, and why a relist makes dropped events survivable
- [ ] What `resourceVersion`, `generation` and `observedGeneration` each mean, and which writes bump which
- [ ] Why the status subresource exists (status writes don't bump generation; separate RBAC)
- [ ] The 409 flow: stale cache → Conflict → re-read key → backoff → retry
- [ ] Work queue guarantees: dedup, dirty/processing sets, no parallel same key, `forget` resetting backoff
- [ ] Exponential backoff with jitter, and why jitter matters (avoid synchronized retries)
- [ ] Lease election: observed-time expiry, renew deadline < lease duration, what goes wrong with long pauses
- [ ] Owner references (`controller: true`), GC, and why I check ownership before every write
- [ ] Finalizer lifecycle: deletionTimestamp set → cleanup → finalizer removed → object gone
- [ ] Rollback rule: `ProgressDeadlineExceeded` for the current generation, `failedImage`/`lastGoodImage`/`appliedImage`
- [ ] The three chaos-found bugs (stale-cache 409s, forgotten last-good image, no-Lease status ping-pong) and their fixes
- [ ] Every invariant the chaos checker enforces, and that "no duplicates" is mostly structural
- [ ] Which numbers are simulated (v2) and which are real k3s (v1, 0.869 s max, API objects not Pods)
- [ ] Why the simple poller wins on median unhealthy time, and what v2 trades for that
