"""Live Kubernetes runtime for controller v2 (needs `pip install -r requirements.txt`).

    .venv/bin/python k8s_adapter.py --namespace default            # with Lease leader election
    .venv/bin/python k8s_adapter.py --namespace default --no-leader-elect

Same shape as the simulator: watch threads -> rate-limited work queue -> one worker
calling Reconciler.reconcile, gated by a coordination.k8s.io Lease.

Honesty note: the v2 live path was written after the last k3s session and has only
been unit-tested against a mocked client; results-live.json was measured with v1.
"""
from __future__ import annotations

import argparse
import logging
import random
import socket
import threading
import time

from controller import (GROUP, KIND, PLURAL, VERSION, AlreadyExists, ApiError, Conflict, Invalid,
                        NotFound, Reconciler, ServerError)
from leader import LeaseElector, micro_time
from workqueue import RateLimiter, WorkQueue

LEASE_NAME = "inference-recovery-operator"


def translate(exc) -> ApiError:
    status = getattr(exc, "status", 500) or 500
    reason = ""
    body = getattr(exc, "body", None)
    if isinstance(body, (str, bytes)) and b"AlreadyExists" in (body if isinstance(body, bytes) else body.encode()):
        reason = "AlreadyExists"
    if status == 404:
        return NotFound(str(exc))
    if status == 409:
        return AlreadyExists(str(exc)) if reason == "AlreadyExists" else Conflict(str(exc))
    if status == 422:
        return Invalid(str(exc))
    return ServerError(str(exc), status=status)


class KubernetesAPI:
    """ResourceAPI over the official client. Clients can be injected for tests."""

    def __init__(self, apps=None, core=None, custom=None, coordination=None, serializer=None, api_exception=None):
        if apps is None:
            from kubernetes import client, config
            from kubernetes.client.rest import ApiException
            config.load_kube_config()
            apps, core, custom = client.AppsV1Api(), client.CoreV1Api(), client.CustomObjectsApi()
            coordination, serializer, api_exception = client.CoordinationV1Api(), client.ApiClient(), ApiException
            self.client = client
        self.apps, self.core, self.custom, self.coordination = apps, core, custom, coordination
        self.serializer, self.ApiException = serializer, api_exception

    def _call(self, fn, *args, **kw):
        try:
            out = fn(*args, **kw)
        except self.ApiException as exc:
            raise translate(exc) from exc
        if out is None or isinstance(out, dict):
            return out
        return self.serializer.sanitize_for_serialization(out)

    _VERBS = ("read", "list", "create", "replace", "replace_status", "delete")
    _TARGET = {"Deployment": ("apps", "deployment"), "Service": ("core", "service"),
               "Lease": ("coordination", "lease")}

    def _op(self, kind: str, verb: str):
        """e.g. ('Deployment', 'replace') -> self.apps.replace_namespaced_deployment"""
        attr, noun = self._TARGET[kind]
        if verb == "replace_status":
            return getattr(getattr(self, attr), f"replace_namespaced_{noun}_status")
        return getattr(getattr(self, attr), f"{verb}_namespaced_{noun}")

    def get(self, kind, namespace, name):
        try:
            if kind == KIND:
                return self._call(self.custom.get_namespaced_custom_object, GROUP, VERSION, namespace, PLURAL, name)
            return self._call(self._op(kind, "read"), name, namespace)
        except NotFound:
            return None

    def list(self, kind, namespace):
        if kind == KIND:
            return self._call(self.custom.list_namespaced_custom_object, GROUP, VERSION, namespace, PLURAL)["items"]
        return self._call(self._op(kind, "list"), namespace)["items"]

    def create(self, kind, namespace, body):
        if kind == KIND:
            return self._call(self.custom.create_namespaced_custom_object, GROUP, VERSION, namespace, PLURAL, body)
        return self._call(self._op(kind, "create"), namespace, body)

    def update(self, kind, namespace, body):
        name = body["metadata"]["name"]
        if kind == KIND:
            return self._call(self.custom.replace_namespaced_custom_object, GROUP, VERSION, namespace, PLURAL, name, body)
        return self._call(self._op(kind, "replace"), name, namespace, body)

    def update_status(self, kind, namespace, body):
        name = body["metadata"]["name"]
        if kind == KIND:
            return self._call(self.custom.replace_namespaced_custom_object_status, GROUP, VERSION, namespace, PLURAL, name, body)
        return self._call(self._op(kind, "replace_status"), name, namespace, body)

    def delete(self, kind, namespace, name, uid=None):
        opts = {"preconditions": {"uid": uid}} if uid else {}
        if kind == KIND:
            self._call(self.custom.delete_namespaced_custom_object, GROUP, VERSION, namespace, PLURAL, name, body=opts)
        else:
            self._call(self._op(kind, "delete"), name, namespace, body=opts)


class LiveOperator:
    """Threads: one watch per kind, a periodic relist, a lease elector, one worker."""

    def __init__(self, api: KubernetesAPI, namespace: str, leader_elect: bool = True,
                 identity: str | None = None, relist_period: float = 30.0):
        self.api, self.namespace, self.relist_period = api, namespace, relist_period
        self.lock = threading.Condition()
        self.queue = WorkQueue(time.monotonic, RateLimiter(rng=random.Random()))
        self.reconciler = Reconciler(api, now=lambda: micro_time(time.time()))  # direct reads, no cache
        self.identity = identity or f"{socket.gethostname()}-{random.randrange(1 << 30):x}"
        self.elector = LeaseElector(api, namespace, LEASE_NAME, self.identity, time.monotonic) if leader_elect else None
        self.stop = threading.Event()

    def enqueue(self, key) -> None:
        with self.lock:
            self.queue.add(key)
            self.lock.notify()

    def on_object(self, obj: dict) -> None:
        """Map any event to the CR key it concerns: the CR itself, or the owner of a child.
        Children share their owner's name, so an unowned squatter with that name also maps
        to the CR (that is how its deletion wakes us up)."""
        meta = obj.get("metadata") or {}
        name = meta.get("name")
        for ref in meta.get("ownerReferences") or []:
            if ref.get("kind") == KIND and ref.get("controller"):
                name = ref.get("name")
        if name:
            self.enqueue((meta.get("namespace") or self.namespace, name))

    def relist(self) -> None:
        for cr in self.api.list(KIND, self.namespace):
            self.enqueue((cr["metadata"]["namespace"], cr["metadata"]["name"]))

    def _watch(self, kind: str) -> None:
        from kubernetes import watch
        while not self.stop.is_set():
            try:
                w = watch.Watch()
                if kind == KIND:
                    stream = w.stream(self.api.custom.list_namespaced_custom_object, GROUP, VERSION,
                                      self.namespace, PLURAL, timeout_seconds=60)
                elif kind == "Deployment":
                    stream = w.stream(self.api.apps.list_namespaced_deployment, self.namespace, timeout_seconds=60)
                else:
                    stream = w.stream(self.api.core.list_namespaced_service, self.namespace, timeout_seconds=60)
                for event in stream:
                    obj = event["object"]
                    if not isinstance(obj, dict):
                        obj = self.api.serializer.sanitize_for_serialization(obj)
                    obj.setdefault("kind", kind)
                    self.on_object(obj)
                    if self.stop.is_set():
                        w.stop()
            except Exception:
                logging.exception("watch %s failed; relisting", kind)
                time.sleep(1)

    def _relist_loop(self) -> None:
        while not self.stop.wait(self.relist_period):
            try:
                self.relist()
            except ApiError:
                logging.exception("relist failed")

    def _elect_loop(self) -> None:
        while not self.stop.is_set():
            was = self.elector.is_leader
            if self.elector.tick() and not was:
                logging.info("became leader as %s", self.identity)
                self.relist()
            elif was and not self.elector.is_leader:
                logging.warning("lost leadership")
            with self.lock:
                self.lock.notify()
            self.stop.wait(self.elector.retry_period * (1 + 0.2 * random.random()))

    def leading(self) -> bool:
        return self.elector is None or self.elector.is_leader

    def _worker(self) -> None:
        while not self.stop.is_set():
            with self.lock:
                key = self.queue.get() if self.leading() else None
                if key is None:
                    nxt = self.queue.next_ready_time()
                    timeout = 1.0 if nxt is None else max(0.0, min(1.0, nxt - time.monotonic()))
                    self.lock.wait(timeout)
                    continue
            try:
                res = self.reconciler.reconcile(*key)
                for action in res.actions:
                    logging.info("%s/%s: %s", key[0], key[1], action)
                with self.lock:
                    self.queue.forget(key)
                    if res.requeue_after is not None:
                        self.queue.add_after(key, res.requeue_after)
            except ApiError as exc:
                with self.lock:
                    delay = self.queue.add_rate_limited(key)
                logging.warning("%s/%s: %s %s; retry in %.3fs", key[0], key[1], exc.status, exc.reason, delay)
            except Exception:
                with self.lock:
                    self.queue.add_rate_limited(key)
                logging.exception("reconcile crashed for %s", key)
            finally:
                with self.lock:
                    self.queue.done(key)

    def run(self) -> list[threading.Thread]:
        targets = [self._worker, self._relist_loop] + [lambda k=k: self._watch(k) for k in (KIND, "Deployment", "Service")]
        if self.elector:
            targets.append(self._elect_loop)
        else:
            self.relist()
        threads = [threading.Thread(target=t, daemon=True) for t in targets]
        for t in threads:
            t.start()
        return threads


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--no-leader-elect", action="store_true")
    parser.add_argument("--relist", type=float, default=30.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    op = LiveOperator(KubernetesAPI(), args.namespace, not args.no_leader_elect, relist_period=args.relist)
    op.run()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        op.stop.set()


if __name__ == "__main__":
    main()
