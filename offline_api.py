"""A simulated-clock fake of the bits of the Kubernetes API server this operator uses.

Everything runs on a discrete-event scheduler (``Sim``), so a "120 s" scenario takes
milliseconds and is fully reproducible from a seed. Modelled semantics:

  * uid / resourceVersion / generation (generation bumps on spec change only);
  * resourceVersion preconditions -> 409 Conflict; create of an existing name -> 409 AlreadyExists;
  * status subresource: ``update`` ignores status, ``update_status`` ignores everything else;
  * finalizers + deletionTimestamp; ownerReference garbage collection (background, delayed);
  * a fake Deployment controller (rollout delay, images tagged ``-broken`` never become
    available and hit ProgressDeadlineExceeded);
  * per-client latency and injected 5xx errors; watch streams with FIFO delay, breaks
    (followed by a relist) and silent drops;
  * an audit log of every request, used by the invariant checker.

Simplifications (also listed in the README): a whole reconcile executes atomically at
one simulated instant (its cost only delays that worker's next item); there is no
admission webhook, field manager or real Pod model. This is a test harness, not a
Kubernetes implementation, and simulated seconds are not real seconds.
"""
from __future__ import annotations

import heapq
import random
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable

from controller import (KIND, AlreadyExists, ApiError, Conflict, Invalid, NotFound, ServerError,
                        container_image, controller_owner_uid)
from leader import micro_time

CONTROLLER_KINDS_WITH_STATUS = {"Deployment", KIND}


# --------------------------------------------------------------------------- scheduler
class Sim:
    def __init__(self):
        self.now = 0.0
        self._heap: list[tuple[float, int, Callable[[], None]]] = []
        self._seq = 0
        self.observers: list[Callable[[], None]] = []
        self.events_run = 0

    def at(self, t: float, fn: Callable[[], None]) -> None:
        self._seq += 1
        heapq.heappush(self._heap, (max(t, self.now), self._seq, fn))

    def after(self, dt: float, fn: Callable[[], None]) -> None:
        self.at(self.now + dt, fn)

    def run_until(self, t_end: float) -> None:
        while self._heap and self._heap[0][0] <= t_end:
            t, _, fn = heapq.heappop(self._heap)
            self.now = t
            fn()
            self.events_run += 1
            for obs in self.observers:
                obs()
        self.now = t_end


# --------------------------------------------------------------------------- API server
@dataclass
class AuditEntry:
    t: float
    actor: str
    verb: str
    kind: str
    name: str
    code: int
    target_owner: str | None = None      # controller ownerRef uid of the object *before* the write
    target_existed: bool = False
    lease_holder_ok: bool | None = None  # for controller writes: was actor the unexpired Lease holder?


@dataclass
class WatchStream:
    deliver: Callable[[str, dict], None]
    delay: Callable[[], float]
    drop_prob: float = 0.0
    epoch: int = 0
    broken_until: float = -1.0
    last_delivery: float = 0.0
    active: bool = True


@dataclass
class ChaosConfig:
    latency: tuple[float, float] = (0.001, 0.005)
    error_windows: list[tuple[float, float, float]] = field(default_factory=list)  # (t0, t1, p)
    rollout_delay: tuple[float, float] = (1.0, 4.0)
    gc_delay: float = 0.5


class FakeAPIServer:
    def __init__(self, sim: Sim, rng: random.Random, chaos: ChaosConfig | None = None):
        self.sim, self.rng = sim, rng
        self.chaos = chaos or ChaosConfig()
        self.objects: dict[tuple[str, str, str], dict] = {}
        self.rv = 0
        self.uid_seq = 0
        self.streams: list[WatchStream] = []
        self.audit: list[AuditEntry] = []
        self.lease_name = None  # (namespace, name) of the election Lease, for audit
        self.listeners: list[Callable[[str, str, dict], None]] = []  # in-process controllers (no chaos)

    # -- helpers
    def _next_rv(self) -> str:
        self.rv += 1
        return str(self.rv)

    def raw(self, kind: str, namespace: str, name: str) -> dict | None:
        return self.objects.get((kind, namespace, name))

    def client(self, actor: str, faulty: bool = False) -> "Client":
        return Client(self, actor, faulty)

    def watch(self, deliver: Callable[[str, dict], None], delay: Callable[[], float],
              drop_prob: float = 0.0) -> WatchStream:
        s = WatchStream(deliver, delay, drop_prob)
        self.streams.append(s)
        return s

    def _emit(self, etype: str, obj: dict) -> None:
        for listener in list(self.listeners):
            listener(etype, obj["kind"], obj)
        for s in list(self.streams):
            if not s.active:
                continue
            if self.sim.now < s.broken_until or (s.drop_prob and self.rng.random() < s.drop_prob):
                continue
            t = max(s.last_delivery, self.sim.now + s.delay())
            s.last_delivery = t
            epoch, snapshot = s.epoch, deepcopy(obj)

            def fire(s=s, epoch=epoch, etype=etype, snapshot=snapshot):
                if s.active and s.epoch == epoch:
                    s.deliver(etype, snapshot)
            self.sim.at(t, fire)

    def _lease_holder_ok(self, actor: str) -> bool | None:
        if not self.lease_name:
            return None
        lease = self.raw("Lease", *self.lease_name)
        return bool(lease and lease["spec"].get("holderIdentity") == actor)

    def _record(self, actor, verb, kind, name, code, before=None, controller=False):
        self.audit.append(AuditEntry(
            self.sim.now, actor, verb, kind, name, code,
            controller_owner_uid(before) if before else None, before is not None,
            self._lease_holder_ok(actor) if controller and verb in ("create", "update", "update_status", "delete", "patch") and kind != "Lease" else None))

    # -- verbs (no chaos here; Client adds latency + injected errors)
    def get(self, kind, namespace, name):
        obj = self.raw(kind, namespace, name)
        return deepcopy(obj) if obj is not None else None

    def list(self, kind, namespace):
        return [deepcopy(o) for (k, ns, _), o in sorted(self.objects.items()) if k == kind and ns == namespace]

    def create(self, kind, namespace, body):
        name = body["metadata"]["name"]
        if (kind, namespace, name) in self.objects:
            raise AlreadyExists(f"{kind} {namespace}/{name}")
        obj = deepcopy(body)
        obj["kind"] = kind
        meta = obj["metadata"]
        meta["namespace"] = namespace
        self.uid_seq += 1
        meta["uid"] = f"uid-{self.uid_seq}"
        meta["resourceVersion"] = self._next_rv()
        meta["generation"] = 1
        meta["creationTimestamp"] = micro_time(self.sim.now)
        meta.pop("deletionTimestamp", None)
        if kind in CONTROLLER_KINDS_WITH_STATUS:
            obj["status"] = {}
        self.objects[(kind, namespace, name)] = obj
        self._emit("ADDED", obj)
        return deepcopy(obj)

    def _existing(self, kind, namespace, body):
        name = body["metadata"]["name"]
        cur = self.raw(kind, namespace, name)
        if cur is None:
            raise NotFound(f"{kind} {namespace}/{name}")
        want_rv = body["metadata"].get("resourceVersion")
        if want_rv and want_rv != cur["metadata"]["resourceVersion"]:
            raise Conflict(f"{kind} {namespace}/{name}: resourceVersion {want_rv} is stale")
        uid = body["metadata"].get("uid")
        if uid and uid != cur["metadata"]["uid"]:
            raise Conflict(f"{kind} {namespace}/{name}: uid precondition failed")
        return cur

    def update(self, kind, namespace, body):
        cur = self._existing(kind, namespace, body)
        new = deepcopy(body)
        new["kind"] = kind
        meta = new["metadata"]
        for k in ("uid", "creationTimestamp", "generation", "deletionTimestamp", "namespace"):
            if k in cur["metadata"]:
                meta[k] = cur["metadata"][k]
            else:
                meta.pop(k, None)
        if kind == "Deployment":
            old_sel = (cur.get("spec") or {}).get("selector")
            if (new.get("spec") or {}).get("selector") != old_sel:
                raise Invalid("spec.selector is immutable")
        if kind in CONTROLLER_KINDS_WITH_STATUS:
            new["status"] = deepcopy(cur.get("status") or {})
        return self._commit(cur, new)

    def update_status(self, kind, namespace, body):
        cur = self._existing(kind, namespace, body)
        new = deepcopy(cur)
        new["status"] = deepcopy(body.get("status") or {})
        return self._commit(cur, new)

    def patch(self, kind, namespace, name, patch):
        """JSON merge patch, no precondition (what `kubectl edit/patch` by a human looks like)."""
        cur = self.raw(kind, namespace, name)
        if cur is None:
            raise NotFound(f"{kind} {namespace}/{name}")
        new = deepcopy(cur)
        _merge(new, deepcopy(patch))
        if kind == "Deployment" and (new.get("spec") or {}).get("selector") != (cur.get("spec") or {}).get("selector"):
            raise Invalid("spec.selector is immutable")
        return self._commit(cur, new)

    def _commit(self, cur, new):
        if new.get("spec") != cur.get("spec"):
            new["metadata"]["generation"] = cur["metadata"].get("generation", 1) + 1
        if new == cur:
            return deepcopy(cur)  # true no-op: real API servers also skip the write
        new["metadata"]["resourceVersion"] = self._next_rv()
        key = (new["kind"], new["metadata"]["namespace"], new["metadata"]["name"])
        if new["metadata"].get("deletionTimestamp") and not new["metadata"].get("finalizers"):
            del self.objects[key]
            self._emit("DELETED", new)
            self._schedule_gc(new["metadata"]["uid"])
            return deepcopy(new)
        self.objects[key] = new
        self._emit("MODIFIED", new)
        return deepcopy(new)

    def delete(self, kind, namespace, name, uid=None):
        cur = self.raw(kind, namespace, name)
        if cur is None:
            raise NotFound(f"{kind} {namespace}/{name}")
        if uid and cur["metadata"]["uid"] != uid:
            raise Conflict("uid precondition failed")
        if cur["metadata"].get("finalizers"):
            if not cur["metadata"].get("deletionTimestamp"):
                new = deepcopy(cur)
                new["metadata"]["deletionTimestamp"] = micro_time(self.sim.now)
                new["metadata"]["resourceVersion"] = self._next_rv()
                self.objects[(kind, namespace, name)] = new
                self._emit("MODIFIED", new)
            return
        del self.objects[(kind, namespace, name)]
        gone = deepcopy(cur)
        gone["metadata"]["resourceVersion"] = self._next_rv()
        self._emit("DELETED", gone)
        self._schedule_gc(cur["metadata"]["uid"])

    def _schedule_gc(self, owner_uid: str) -> None:
        def gc():
            if any(o["metadata"]["uid"] == owner_uid for o in self.objects.values()):
                return
            for (kind, ns, name), obj in list(self.objects.items()):
                refs = obj["metadata"].get("ownerReferences") or []
                if any(r.get("uid") == owner_uid for r in refs):
                    try:
                        before = deepcopy(obj)
                        self.delete(kind, ns, name)
                        self._record("garbage-collector", "delete", kind, name, 200, before)
                    except ApiError:
                        pass
        self.sim.after(self.chaos.gc_delay, gc)


def _merge(target: dict, patch: dict) -> None:
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge(target[key], value)
        else:
            target[key] = value


class Client:
    """What one process sees: latency, injected 5xx, and an audit trail of its requests."""

    def __init__(self, server: FakeAPIServer, actor: str, faulty: bool):
        self.server, self.actor, self.faulty = server, actor, faulty
        self.cost = 0.0
        self.requests = 0

    def _call(self, verb, kind, name, fn, *args, **kw):
        s = self.server
        self.cost += s.rng.uniform(*s.chaos.latency)
        self.requests += 1
        before = s.raw(kind, args[1], name) if verb in ("update", "update_status", "delete", "patch") else None
        if self.faulty:
            for t0, t1, p in s.chaos.error_windows:
                if t0 <= s.sim.now < t1 and s.rng.random() < p:
                    s._record(self.actor, verb, kind, name, 503, before, controller=True)
                    raise ServerError("injected 503")
        try:
            out = fn(*args, **kw)
            s._record(self.actor, verb, kind, name, 404 if (verb == "get" and out is None) else 200, deepcopy(before) if before else None, controller=self.faulty)
            return out
        except ApiError as exc:
            s._record(self.actor, verb, kind, name, exc.status, before, controller=self.faulty)
            raise

    def get(self, kind, namespace, name):
        return self._call("get", kind, name, self.server.get, kind, namespace, name)

    def list(self, kind, namespace):
        return self._call("list", kind, "*", lambda k, ns: self.server.list(k, ns), kind, namespace)

    def create(self, kind, namespace, body):
        return self._call("create", kind, body["metadata"]["name"], self.server.create, kind, namespace, body)

    def update(self, kind, namespace, body):
        return self._call("update", kind, body["metadata"]["name"], self.server.update, kind, namespace, body)

    def update_status(self, kind, namespace, body):
        return self._call("update_status", kind, body["metadata"]["name"], self.server.update_status, kind, namespace, body)

    def delete(self, kind, namespace, name, uid=None):
        return self._call("delete", kind, name, self.server.delete, kind, namespace, name, uid=uid)

    def patch(self, kind, namespace, name, patch):
        return self._call("patch", kind, name, self.server.patch, kind, namespace, name, patch)


# --------------------------------------------------------------------------- informer
class Informer:
    """List + watch into a local cache. Reads come from here, so they can be stale."""

    def __init__(self, sim: Sim, client: Client, namespace: str, kinds: tuple[str, ...],
                 on_event: Callable[[str, dict], None], delay: Callable[[], float],
                 drop_prob: float = 0.0, relist_period: float = 30.0):
        self.sim, self.client, self.namespace, self.kinds = sim, client, namespace, kinds
        self.on_event, self.relist_period = on_event, relist_period
        self.store: dict[tuple[str, str, str], dict] = {}
        self.stream = client.server.watch(self._deliver, delay, drop_prob)
        self.active = True
        self.relists = 0

    def start(self) -> None:
        self.relist()
        self._schedule_periodic()

    def stop(self) -> None:
        self.active = False
        self.stream.active = False

    def _schedule_periodic(self) -> None:
        def tick():
            if self.active:
                self.relist()
                self._schedule_periodic()
        self.sim.after(self.relist_period, tick)

    def break_watch(self, until: float) -> None:
        """Connection drops; events are lost until ``until``, then the informer relists."""
        self.stream.broken_until = until
        self.sim.at(until, lambda: self.active and self.relist())

    def relist(self) -> None:
        if not self.active:
            return
        try:
            fresh = {}
            for kind in self.kinds:
                for obj in self.client.list(kind, self.namespace):
                    fresh[(kind, self.namespace, obj["metadata"]["name"])] = obj
        except ApiError:
            self.sim.after(1.0, self.relist)  # list failed: retry soon
            return
        self.relists += 1
        self.stream.epoch += 1          # in-flight events from the old watch are discarded
        self.stream.last_delivery = self.sim.now
        old, self.store = self.store, fresh
        for key, obj in old.items():
            if key not in fresh:
                self.on_event("DELETED", obj)
        for obj in fresh.values():
            self.on_event("SYNC", obj)  # level-triggered resync: everything gets re-enqueued

    def _deliver(self, etype: str, obj: dict) -> None:
        if not self.active or obj["kind"] not in self.kinds:
            return
        key = (obj["kind"], obj["metadata"]["namespace"], obj["metadata"]["name"])
        if etype == "DELETED":
            self.store.pop(key, None)
        else:
            self.store[key] = obj
        self.on_event(etype, obj)

    def refresh(self, namespace: str, name: str) -> None:
        """Targeted re-read after a 409: our cache is provably stale for this key."""
        for kind in self.kinds:
            try:
                obj = self.client.get(kind, namespace, name)
            except ApiError:
                return
            if obj is None:
                self.store.pop((kind, namespace, name), None)
            else:
                self.store[(kind, namespace, name)] = obj

    def get(self, kind, namespace, name):
        obj = self.store.get((kind, namespace, name))
        return deepcopy(obj) if obj is not None else None

    def keys(self, kind):
        return [k for k in self.store if k[0] == kind]


# --------------------------------------------------------------------------- kube-controller-manager stand-in
class FakeDeploymentController:
    """Marks Deployments available after a delay; images containing 'broken' never get there."""

    def __init__(self, sim: Sim, server: FakeAPIServer):
        self.sim, self.server = sim, server
        server.listeners.append(self._on_change)

    def _on_change(self, etype, kind, obj):
        if kind != "Deployment" or etype == "DELETED":
            return
        meta = obj["metadata"]
        gen = meta["generation"]
        if (obj.get("status") or {}).get("observedGeneration") == gen:
            return
        key = (meta["namespace"], meta["name"], meta["uid"])
        self.sim.after(0.05, lambda: self._observe(key, gen))

    def _current(self, key, gen):
        obj = self.server.raw("Deployment", key[0], key[1])
        if obj is None or obj["metadata"]["uid"] != key[2] or obj["metadata"]["generation"] != gen:
            return None
        return obj

    def _write(self, obj, status):
        body = deepcopy(obj)
        body["status"] = status
        self.server.update_status("Deployment", obj["metadata"]["namespace"], body)

    def _observe(self, key, gen):
        obj = self._current(key, gen)
        if obj is None or (obj.get("status") or {}).get("observedGeneration") == gen:
            return
        old = obj.get("status") or {}
        replicas = obj["spec"].get("replicas", 1)
        image = container_image(obj) or ""
        prev_avail = min(old.get("availableReplicas", 0), replicas)
        self._write(obj, {"observedGeneration": gen, "replicas": replicas, "updatedReplicas": 0,
                          "availableReplicas": prev_avail, "lastImage": old.get("lastImage"),
                          "conditions": [{"type": "Progressing", "status": "True", "reason": "ReplicaSetUpdated"}]})
        if "broken" in image:
            deadline = obj["spec"].get("progressDeadlineSeconds", 600)
            self.sim.after(deadline, lambda: self._fail(key, gen))
        else:
            self.sim.after(self.server.rng.uniform(*self.server.chaos.rollout_delay), lambda: self._finish(key, gen))

    def _finish(self, key, gen):
        obj = self._current(key, gen)
        if obj is None:
            return
        replicas = obj["spec"].get("replicas", 1)
        self._write(obj, {"observedGeneration": gen, "replicas": replicas, "updatedReplicas": replicas,
                          "availableReplicas": replicas, "lastImage": container_image(obj),
                          "conditions": [{"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable"},
                                         {"type": "Available", "status": "True", "reason": "MinimumReplicasAvailable"}]})

    def _fail(self, key, gen):
        obj = self._current(key, gen)
        if obj is None:
            return
        status = deepcopy(obj.get("status") or {})
        status["conditions"] = [{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}]
        self._write(obj, status)
