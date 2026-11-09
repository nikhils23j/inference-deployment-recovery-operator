"""Reconciler behaviour against the fake API server (no chaos; reads go straight to the API)."""
import random
import unittest
from copy import deepcopy

from controller import (FINALIZER, KIND, Conflict, Desired, Reconciler, condition, container_image)
from offline_api import FakeAPIServer, FakeDeploymentController, Sim

NS, NAME = "default", "demo-inference"
SPEC = {"image": "registry.local/model:v1", "replicas": 2, "port": 8080, "progressDeadlineSeconds": 20}


class Base(unittest.TestCase):
    def setUp(self):
        self.sim = Sim()
        self.server = FakeAPIServer(self.sim, random.Random(1))
        self.ctrl = self.server.client("ctrl-a")
        self.user = self.server.client("user")
        self.cr = self.user.create(KIND, NS, {"metadata": {"name": NAME}, "spec": dict(SPEC)})
        self.r = Reconciler(self.ctrl, now=lambda: f"t={self.sim.now}")

    def dep(self):
        return self.server.raw("Deployment", NS, NAME)

    def svc(self):
        return self.server.raw("Service", NS, NAME)

    def crobj(self):
        return self.server.raw(KIND, NS, NAME)

    def writes(self):
        return sum(1 for a in self.server.audit if a.actor == "ctrl-a" and a.verb != "get" and a.verb != "list")


class ReconcileTests(Base):
    def test_creates_children_finalizer_and_status_then_noop(self):
        res = self.r.reconcile(NS, NAME)
        self.assertEqual(res.actions[:3], ["added finalizer", "created Deployment", "created Service"])
        self.assertIn(FINALIZER, self.crobj()["metadata"]["finalizers"])
        self.assertEqual(self.crobj()["status"]["observedGeneration"], 1)
        before = self.writes()
        rv = self.crobj()["metadata"]["resourceVersion"]
        self.sim.now = 5.0  # time moves on; lastTransitionTime must not, or status would hot-loop
        self.assertEqual(self.r.reconcile(NS, NAME).actions, [])
        self.assertEqual(self.writes(), before)
        self.assertEqual(self.crobj()["metadata"]["resourceVersion"], rv)

    def test_restores_deleted_children(self):
        self.r.reconcile(NS, NAME)
        for kind in ("Deployment", "Service"):
            self.user.delete(kind, NS, NAME)
            self.assertIn(f"created {kind}", self.r.reconcile(NS, NAME).actions)
        owned = [o for o in self.server.objects.values() if o["kind"] in ("Deployment", "Service")]
        self.assertEqual(len(owned), 2)

    def test_repairs_drift_but_keeps_unmanaged_fields(self):
        self.r.reconcile(NS, NAME)
        self.user.patch("Deployment", NS, NAME, {"metadata": {"annotations": {"team": "ml"}},
                                                  "spec": {"replicas": 9}})
        dep = self.dep()
        c = dict(dep["spec"]["template"]["spec"]["containers"][0], image="wrong:latest", env=[{"name": "A", "value": "1"}])
        self.user.patch("Deployment", NS, NAME, {"spec": {"template": {"spec": {"containers": [c]}}}})
        self.user.patch("Service", NS, NAME, {"spec": {"selector": {"wrong": "app"},
                                                       "ports": [{"name": "http", "port": 81, "targetPort": 81}]}})
        actions = self.r.reconcile(NS, NAME).actions
        self.assertTrue(any(a.startswith("patched Deployment") for a in actions))
        self.assertTrue(any(a.startswith("patched Service") for a in actions))
        dep, svc = self.dep(), self.svc()
        self.assertEqual(dep["spec"]["replicas"], 2)
        self.assertEqual(container_image(dep), SPEC["image"])
        self.assertEqual(dep["metadata"]["annotations"], {"team": "ml"})               # not ours: kept
        self.assertEqual(dep["spec"]["template"]["spec"]["containers"][0]["env"][0]["name"], "A")
        self.assertEqual(svc["spec"]["selector"], {"app.kubernetes.io/name": NAME})   # stale key removed
        self.assertEqual(svc["spec"]["ports"][0]["port"], 8080)
        self.assertEqual(self.r.reconcile(NS, NAME).actions, [])

    def test_refuses_unowned_object_and_reports_it(self):
        squatter = self.user.create("Deployment", NS, {
            "metadata": {"name": NAME}, "spec": {"replicas": 1, "selector": {"matchLabels": {"a": "b"}},
                                                 "template": {"spec": {"containers": [{"name": "x", "image": "theirs"}]}}}})
        res = self.r.reconcile(NS, NAME)
        self.assertEqual(res.requeue_after, Reconciler.BLOCKED_REQUEUE_S)
        self.assertEqual(self.dep()["metadata"]["resourceVersion"], squatter["metadata"]["resourceVersion"])
        self.assertEqual(condition(self.crobj(), "Degraded")["reason"], "ChildNotOwned")
        self.assertIsNotNone(self.svc())  # the Service is still ours and still created

    def test_stale_read_gets_409_instead_of_clobbering(self):
        self.r.reconcile(NS, NAME)
        stale = {("Deployment", NS, NAME): deepcopy(self.dep()), (KIND, NS, NAME): deepcopy(self.crobj()),
                 ("Service", NS, NAME): deepcopy(self.svc())}
        self.user.patch("Deployment", NS, NAME, {"spec": {"replicas": 9}})  # someone writes after our read
        stale[("Deployment", NS, NAME)]["spec"]["replicas"] = 5            # our cache shows drift too
        cached = Reconciler(self.ctrl, reader=type("R", (), {"get": lambda s, k, n, m: deepcopy(stale.get((k, n, m)))})())
        with self.assertRaises(Conflict):
            cached.reconcile(NS, NAME)
        self.assertEqual(self.dep()["spec"]["replicas"], 9)  # nothing overwritten blindly
        self.r.reconcile(NS, NAME)                           # fresh read fixes it
        self.assertEqual(self.dep()["spec"]["replicas"], 2)

    def test_invalid_spec_reports_and_creates_nothing(self):
        self.user.patch(KIND, NS, NAME, {"spec": {"replicas": 50}})
        self.r.reconcile(NS, NAME)
        self.assertIsNone(self.dep())
        self.assertEqual(condition(self.crobj(), "Degraded")["reason"], "InvalidSpec")

    def test_validates_bounds(self):
        meta = {"namespace": NS, "name": NAME, "uid": "u"}
        for spec in ({"image": ""}, {"image": "x", "replicas": 11}, {"image": "x", "port": 0},
                     {"image": "x", "progressDeadlineSeconds": 1}):
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                Desired.from_resource({"metadata": meta, "spec": spec})


class FinalizerTests(Base):
    def test_cleanup_deletes_only_owned_children_then_releases(self):
        self.r.reconcile(NS, NAME)
        self.user.delete("Service", NS, NAME)
        foreign = self.user.create("Service", NS, {"metadata": {"name": NAME}, "spec": {"ports": [{"port": 1}]}})
        self.user.delete(KIND, NS, NAME)
        self.assertIsNotNone(self.crobj()["metadata"]["deletionTimestamp"])  # held by our finalizer
        actions = self.r.reconcile(NS, NAME).actions
        self.assertEqual(actions, ["deleted Deployment", "removed finalizer"])
        self.assertIsNone(self.crobj())
        self.assertIsNone(self.dep())
        self.assertEqual(self.svc()["metadata"]["uid"], foreign["metadata"]["uid"])  # not ours: untouched


class RollbackTests(Base):
    def setUp(self):
        super().setUp()
        FakeDeploymentController(self.sim, self.server)

    def settle(self, seconds):
        """Advance simulated time, reconciling whenever anything changes (a tiny controller loop)."""
        end = self.sim.now + seconds
        while True:
            res = self.r.reconcile(NS, NAME)
            if res.requeue_after == 0.0:
                continue
            if not self.sim._heap or self.sim._heap[0][0] > end:
                break
            self.sim.run_until(self.sim._heap[0][0])
        self.sim.run_until(end)
        self.r.reconcile(NS, NAME)

    def test_broken_image_rolls_back_to_last_good_then_rolls_forward(self):
        self.settle(10)
        self.assertEqual(condition(self.crobj(), "Ready")["status"], "True")
        self.assertEqual(self.crobj()["status"]["lastGoodImage"], SPEC["image"])

        self.user.patch(KIND, NS, NAME, {"spec": {"image": "registry.local/model:v2-broken"}})
        self.settle(5)
        self.assertEqual(container_image(self.dep()), "registry.local/model:v2-broken")  # trying it
        self.settle(30)                                                                 # deadline is 20 s
        st = self.crobj()["status"]
        self.assertEqual(st["failedImage"], "registry.local/model:v2-broken")
        self.assertEqual(container_image(self.dep()), SPEC["image"])                    # rolled back
        self.assertEqual(condition(self.crobj(), "Degraded")["reason"], "RolledBack")
        self.assertEqual(condition(self.crobj(), "Ready")["status"], "False")

        self.user.patch(KIND, NS, NAME, {"spec": {"image": "registry.local/model:v3"}})
        self.settle(10)
        self.assertEqual(container_image(self.dep()), "registry.local/model:v3")
        self.assertNotIn("failedImage", self.crobj()["status"])
        self.assertEqual(condition(self.crobj(), "Ready")["status"], "True")

    def test_last_good_recorded_even_if_we_never_saw_rollout_finish(self):
        """Regression for chaos seed 72: controller missed the 'rollout complete' moment, then the
        user pushed a broken image. It must still know v1 was good, or it has nothing to roll back to."""
        self.r.reconcile(NS, NAME)                     # applies v1; rollout not finished yet
        self.assertNotIn("lastGoodImage", self.crobj()["status"])
        self.user.patch(KIND, NS, NAME, {"spec": {"image": "registry.local/model:v2-broken"}})
        self.sim.run_until(self.sim.now + 10)          # v1 finishes rolling out while we are not looking
        self.settle(40)
        self.assertEqual(self.crobj()["status"]["lastGoodImage"], SPEC["image"])
        self.assertEqual(container_image(self.dep()), SPEC["image"])
        self.assertEqual(condition(self.crobj(), "Degraded")["reason"], "RolledBack")

    def test_drift_to_broken_image_is_not_blamed_on_spec(self):
        self.settle(10)
        dep = self.dep()
        c = dict(dep["spec"]["template"]["spec"]["containers"][0], image="someone/else-broken")
        self.user.patch("Deployment", NS, NAME, {"spec": {"template": {"spec": {"containers": [c]}}}})
        self.sim.run_until(self.sim.now + 25)  # controller asleep: the drifted image hits its deadline
        self.assertEqual(self.dep()["status"]["conditions"][0]["reason"], "ProgressDeadlineExceeded")
        self.settle(40)
        self.assertNotIn("failedImage", self.crobj()["status"])
        self.assertEqual(container_image(self.dep()), SPEC["image"])


class FakeApiSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeAPIServer(Sim(), random.Random(0))
        self.obj = self.server.create(KIND, NS, {"metadata": {"name": "x"}, "spec": {"image": "a"}})

    def test_generation_bumps_on_spec_only_and_status_is_separate(self):
        body = deepcopy(self.obj)
        body["status"] = {"hello": 1}
        out = self.server.update(KIND, NS, body)              # main endpoint ignores status
        self.assertEqual(out["status"], {})
        out = self.server.update_status(KIND, NS, body)
        self.assertEqual((out["status"], out["metadata"]["generation"]), ({"hello": 1}, 1))
        body = deepcopy(out)
        body["spec"]["image"] = "b"
        self.assertEqual(self.server.update(KIND, NS, body)["metadata"]["generation"], 2)

    def test_stale_resource_version_conflicts(self):
        self.server.patch(KIND, NS, "x", {"spec": {"image": "c"}})
        with self.assertRaises(Conflict):
            self.server.update(KIND, NS, self.obj)


class V1PreservedTests(unittest.TestCase):
    """The v1 code that produced results-live.json still behaves as it did."""

    def test_v1_reconcile_unchanged(self):
        from v1.controller_v1 import reconcile as v1_reconcile
        from v1.offline_api_v1 import MemoryAPI
        res = {"metadata": {"namespace": NS, "name": NAME, "uid": "u1"},
               "spec": {"image": "nginx:1.27-alpine", "replicas": 2, "port": 80}}
        api = MemoryAPI()
        self.assertEqual(v1_reconcile(api, res), ["created Deployment", "created Service"])
        api.delete("Deployment", NS, NAME)
        self.assertEqual(v1_reconcile(api, res), ["created Deployment"])
        self.assertEqual(v1_reconcile(api, res), [])


if __name__ == "__main__":
    unittest.main()
