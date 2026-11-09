"""Controller processes ("replicas") running inside the simulator.

``V2Replica`` is the real runtime shape: informer cache -> event handler -> rate-limited
work queue -> single worker calling ``Reconciler.reconcile``, gated by a Lease elector.
``PollerReplica`` runs the v1 or naive baseline as a fixed-interval polling loop.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from baselines import V1Adapter, naive_reconcile, v1_reconcile
from controller import KIND, AlreadyExists, ApiError, Conflict, NotFound, Reconciler, controller_owner_uid
from leader import LeaseElector, micro_time
from offline_api import FakeAPIServer, Informer, Sim
from workqueue import RateLimiter, WorkQueue

LEASE_NAME = "inference-recovery-operator"


@dataclass
class SimConfig:
    namespace: str = "default"
    watch_delay_max: float = 0.05
    watch_drop_prob: float = 0.0
    relist_period: float = 30.0
    lease_duration: float = 15.0
    renew_deadline: float = 10.0
    retry_period: float = 2.0
    poll_interval: float = 0.5       # v1 evaluator and naive baseline
    naive_retry: float = 0.01        # "retry immediately"
    backoff_cap: float = 30.0


@dataclass
class Metrics:
    reconciles: list[tuple[float, str]] = field(default_factory=list)
    reconcile_errors: int = 0
    max_backoff: float = 0.0


class V2Replica:
    def __init__(self, sim: Sim, server: FakeAPIServer, identity: str, rng: random.Random,
                 cfg: SimConfig, metrics: Metrics, leader_election: bool = True,
                 refresh_on_conflict: bool = True):
        self.sim, self.server, self.identity, self.rng, self.cfg = sim, server, identity, rng, cfg
        self.metrics, self.leader_election = metrics, leader_election
        self.refresh_on_conflict = refresh_on_conflict
        self.alive = False
        self.epoch = 0
        if leader_election:
            server.lease_name = (cfg.namespace, LEASE_NAME)

    # -- lifecycle
    def start(self) -> None:
        self.alive = True
        self.epoch += 1
        self.client = self.server.client(self.identity, faulty=True)
        self.queue = WorkQueue(lambda: self.sim.now,
                               RateLimiter(cap=self.cfg.backoff_cap, rng=random.Random(self.rng.random())))
        self.informer = Informer(self.sim, self.client, self.cfg.namespace, (KIND, "Deployment", "Service"),
                                 self._on_event, lambda: self.rng.uniform(0, self.cfg.watch_delay_max),
                                 self.cfg.watch_drop_prob, self.cfg.relist_period)
        self.reconciler = Reconciler(self.client, self.informer, now=lambda: micro_time(self.sim.now))
        self.elector = LeaseElector(self.client, self.cfg.namespace, LEASE_NAME, self.identity,
                                    lambda: self.sim.now, self.cfg.lease_duration, self.cfg.renew_deadline,
                                    self.cfg.retry_period) if self.leader_election else None
        self.busy_until = self.sim.now
        self.pump_at: float | None = None
        self.informer.start()
        if self.elector:
            self._elect(self.epoch)
        else:
            self._kick()

    def crash(self) -> None:
        self.alive = False
        self.epoch += 1
        self.informer.stop()

    @property
    def leading(self) -> bool:
        return self.alive and (self.elector is None or self.elector.is_leader)

    # -- leader election loop
    def _elect(self, epoch: int) -> None:
        if not self.alive or epoch != self.epoch:
            return
        was = self.elector.is_leader
        now_leader = self.elector.tick()
        if now_leader and not was:
            for key in self.informer.keys(KIND):
                self.queue.add((key[1], key[2]))
            self._kick()
        jitter = self.rng.uniform(0, 0.2 * self.cfg.retry_period)
        self.sim.after(self.cfg.retry_period + jitter, lambda: self._elect(epoch))

    # -- informer -> queue
    def _on_event(self, etype: str, obj: dict) -> None:
        meta = obj["metadata"]
        if obj["kind"] == KIND:
            self.queue.add((meta["namespace"], meta["name"]))
        else:
            owner = controller_owner_uid(obj)
            for key in self.informer.keys(KIND):
                cr = self.informer.store[key]
                if cr["metadata"]["uid"] == owner or cr["metadata"]["name"] == meta["name"]:
                    self.queue.add((key[1], key[2]))
        self._kick()

    # -- worker
    def _kick(self, at: float | None = None) -> None:
        if not self.alive:
            return
        t = max(self.sim.now if at is None else at, self.busy_until)
        if self.pump_at is not None and self.pump_at <= t:
            return
        self.pump_at = t
        epoch = self.epoch
        self.sim.at(t, lambda: self._pump(epoch, t))

    def _pump(self, epoch: int, t: float) -> None:
        if epoch != self.epoch or not self.alive or self.pump_at != t:
            return
        self.pump_at = None
        if not self.leading:
            return  # followers keep their cache warm but never write
        key = self.queue.get()
        if key is None:
            nxt = self.queue.next_ready_time()
            if nxt is not None:
                self._kick(nxt)
            return
        self.client.cost = 0.0
        try:
            res = self.reconciler.reconcile(*key)
            self.queue.forget(key)
            if res.requeue_after is not None:
                self.queue.add_after(key, res.requeue_after)
        except ApiError as exc:
            self.metrics.reconcile_errors += 1
            if self.refresh_on_conflict and isinstance(exc, (Conflict, AlreadyExists, NotFound)):
                self.informer.refresh(*key)
            self.metrics.max_backoff = max(self.metrics.max_backoff, self.queue.add_rate_limited(key))
        finally:
            self.queue.done(key)
        self.metrics.reconciles.append((self.sim.now, self.identity))
        self.busy_until = self.sim.now + max(self.client.cost, 1e-4)
        self._kick()


class PollerReplica:
    """v1 (exact old reconcile) or naive baseline: list + reconcile every poll_interval."""

    def __init__(self, sim: Sim, server: FakeAPIServer, identity: str, cfg: SimConfig,
                 metrics: Metrics, mode: str):
        assert mode in ("v1", "naive")
        self.sim, self.server, self.identity, self.cfg, self.metrics, self.mode = sim, server, identity, cfg, metrics, mode
        self.alive = False
        self.epoch = 0
        self.leading = False

    def start(self) -> None:
        self.alive = self.leading = True
        self.epoch += 1
        self.client = self.server.client(self.identity, faulty=True)
        self._poll(self.epoch)

    def crash(self) -> None:
        self.alive = self.leading = False
        self.epoch += 1

    def _poll(self, epoch: int) -> None:
        if not self.alive or epoch != self.epoch:
            return
        self.client.cost = 0.0
        failed = False
        try:
            for resource in self.client.list(KIND, self.cfg.namespace):
                if (resource.get("metadata") or {}).get("deletionTimestamp"):
                    continue
                try:
                    if self.mode == "v1":
                        v1_reconcile(V1Adapter(self.client), resource)
                    else:
                        naive_reconcile(self.client, resource)
                except (ApiError, ValueError, KeyError):
                    failed = True
                self.metrics.reconciles.append((self.sim.now, self.identity))
        except ApiError:
            failed = True
        if failed:
            self.metrics.reconcile_errors += 1
        delay = self.cfg.naive_retry if (failed and self.mode == "naive") else self.cfg.poll_interval
        self.sim.after(delay + self.client.cost, lambda: self._poll(epoch))
