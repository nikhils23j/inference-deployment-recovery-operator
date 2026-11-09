"""Lease-based leader election, following client-go's leaderelection package.

Expiry is judged by *observed time*: when did this replica last see the Lease
record change? It never trusts the renewTime written by someone else's clock.
The leader steps down if it cannot renew within ``renew_deadline``, which is
shorter than ``lease_duration``, so a healthy follower cannot take over while
the old leader still believes it leads (assuming no process pause longer than
the gap - the classic caveat of every lease scheme).
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Callable

from controller import ApiError, ResourceAPI


def micro_time(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class LeaseElector:
    def __init__(self, api: ResourceAPI, namespace: str, name: str, identity: str,
                 clock: Callable[[], float], lease_duration: float = 15.0,
                 renew_deadline: float = 10.0, retry_period: float = 2.0):
        assert renew_deadline < lease_duration
        self.api, self.namespace, self.name, self.identity = api, namespace, name, identity
        self.clock = clock
        self.lease_duration, self.renew_deadline, self.retry_period = lease_duration, renew_deadline, retry_period
        self.is_leader = False
        self.last_renew = float("-inf")
        self._observed_spec: dict | None = None
        self._observed_at = float("-inf")
        self.transitions = 0

    def tick(self) -> bool:
        """Try to acquire or renew once. Returns whether this replica leads afterwards."""
        now = self.clock()
        try:
            self._try(now)
        except ApiError:
            pass
        if self.is_leader and now - self.last_renew > self.renew_deadline:
            self.is_leader = False  # could not renew in time: stop writing before the lease can expire
        return self.is_leader

    def _try(self, now: float) -> None:
        lease = self.api.get("Lease", self.namespace, self.name)
        spec_new = {"holderIdentity": self.identity, "leaseDurationSeconds": int(self.lease_duration),
                    "renewTime": micro_time(now)}
        if lease is None:
            spec_new.update(acquireTime=micro_time(now), leaseTransitions=0)
            self.api.create("Lease", self.namespace, {
                "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
                "metadata": {"name": self.name, "namespace": self.namespace}, "spec": spec_new})
            self._won(now, spec_new)
            return
        spec = lease.get("spec") or {}
        if spec != self._observed_spec:
            self._observed_spec, self._observed_at = deepcopy(spec), now
        holder = spec.get("holderIdentity")
        duration = spec.get("leaseDurationSeconds") or self.lease_duration
        if holder and holder != self.identity and now < self._observed_at + duration:
            self.is_leader = False
            return  # someone else holds a lease that has not expired from our point of view
        if holder != self.identity:
            spec_new.update(acquireTime=micro_time(now), leaseTransitions=spec.get("leaseTransitions", 0) + 1)
        else:
            spec_new.update(acquireTime=spec.get("acquireTime"), leaseTransitions=spec.get("leaseTransitions", 0))
        body = deepcopy(lease)
        body["spec"] = spec_new
        self.api.update("Lease", self.namespace, body)  # resourceVersion precondition: losers get 409
        self._won(now, spec_new)

    def _won(self, now: float, spec: dict) -> None:
        if not self.is_leader:
            self.transitions += 1
        self.is_leader = True
        self.last_renew = now
        self._observed_spec, self._observed_at = deepcopy(spec), now
