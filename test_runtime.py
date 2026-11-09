"""Work queue, leader election, live-adapter translation, and chaos-harness invariants."""
import random
import unittest

from chaos import make_schedule, run_one
from controller import AlreadyExists, Conflict, NotFound, ServerError
from k8s_adapter import KubernetesAPI, translate
from leader import LeaseElector
from offline_api import ChaosConfig, FakeAPIServer, Sim
from workqueue import RateLimiter, WorkQueue


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class WorkQueueTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.q = WorkQueue(self.clock, RateLimiter(rng=random.Random(0)))

    def test_dedup_and_no_concurrent_processing(self):
        for _ in range(5):
            self.q.add("a")
        self.assertEqual(len(self.q), 1)
        key = self.q.get()
        self.q.add("a")                      # event while processing: marked dirty, not handed out
        self.assertIsNone(self.q.get())
        self.q.done(key)
        self.assertEqual(self.q.get(), "a")  # ...and re-queued exactly once when done

    def test_exponential_backoff_with_jitter_and_cap(self):
        lim = RateLimiter(base=0.005, cap=30.0, qps=1e9, burst=10**9, rng=random.Random(1))
        delays = [lim.when("k", 0.0) for _ in range(20)]
        for n, d in enumerate(delays):
            full = min(30.0, 0.005 * 2 ** n)
            self.assertGreaterEqual(d, 0.5 * full)
            self.assertLessEqual(d, full)
        lim.forget("k")
        self.assertLessEqual(lim.when("k", 0.0), 0.005)

    def test_token_bucket_limits_bursts_across_keys(self):
        lim = RateLimiter(base=0.0, qps=10, burst=5, rng=random.Random(0))
        delays = [lim.when(i, 0.0) for i in range(8)]
        self.assertEqual(delays[:5], [0.0] * 5)
        self.assertAlmostEqual(delays[7], 0.3)

    def test_add_after_releases_in_time_order(self):
        self.q.add_after("late", 2.0)
        self.q.add_after("soon", 1.0)
        self.assertIsNone(self.q.get())
        self.assertEqual(self.q.next_ready_time(), 1.0)
        self.clock.t = 2.5
        self.assertEqual([self.q.get(), self.q.get()], ["soon", "late"])


class LeaderElectionTests(unittest.TestCase):
    def setUp(self):
        self.sim = Sim()
        self.server = FakeAPIServer(self.sim, random.Random(0), ChaosConfig())
        self.clock = lambda: self.sim.now
        self.a = LeaseElector(self.server.client("a", faulty=True), "default", "l", "a", self.clock)
        self.b = LeaseElector(self.server.client("b", faulty=True), "default", "l", "b", self.clock)

    def step_until(self, t, electors):
        while self.sim.now < t:
            self.sim.now = round(self.sim.now + 2.0, 6)
            for e in electors:
                e.tick()
            self.assertLessEqual(sum(e.is_leader for e in (self.a, self.b)), 1, f"two leaders at {self.sim.now}")

    def test_one_leader_and_takeover_only_after_expiry(self):
        self.assertTrue(self.a.tick())
        self.assertFalse(self.b.tick())
        self.step_until(20, [self.a, self.b])
        self.assertTrue(self.a.is_leader)
        crash = self.sim.now                      # a dies: stops ticking
        self.a.is_leader = False
        while not self.b.is_leader:
            self.step_until(self.sim.now + 2, [self.b])
        self.assertGreaterEqual(self.sim.now - crash, 15.0)  # waited a full lease duration

    def test_leader_steps_down_before_follower_can_take_over(self):
        self.a.tick()
        self.b.tick()
        # every request fails for a while (API outage), for both replicas
        self.server.chaos.error_windows.append((self.sim.now + 0.5, self.sim.now + 30, 1.0))
        self.step_until(self.sim.now + 12, [self.a, self.b])
        self.assertFalse(self.a.is_leader)          # renew deadline (10 s) passed
        self.step_until(self.sim.now + 40, [self.a, self.b])
        self.assertEqual(sum(e.is_leader for e in (self.a, self.b)), 1)

    def test_simultaneous_acquire_one_wins_via_conflict(self):
        lease = self.server.client("x").create("Lease", "default", {
            "metadata": {"name": "l"}, "spec": {"holderIdentity": "", "leaseDurationSeconds": 15}})
        self.assertIsNotNone(lease)
        stale = self.server.get("Lease", "default", "l")
        self.assertTrue(self.a.tick())
        stale["spec"]["holderIdentity"] = "b"
        with self.assertRaises(Conflict):
            self.server.update("Lease", "default", stale)


class FakeApiException(Exception):
    def __init__(self, status, body=""):
        self.status, self.body = status, body


class LiveAdapterTranslationTests(unittest.TestCase):
    """Only the mapping layer; the live v2 path has not been run against a real cluster yet."""

    def test_error_translation(self):
        self.assertIsInstance(translate(FakeApiException(404)), NotFound)
        self.assertIsInstance(translate(FakeApiException(409, '{"reason":"Conflict"}')), Conflict)
        self.assertIsInstance(translate(FakeApiException(409, '{"reason":"AlreadyExists"}')), AlreadyExists)
        self.assertIsInstance(translate(FakeApiException(503)), ServerError)

    def test_calls_map_to_client_methods(self):
        calls = []

        class Apps:
            def read_namespaced_deployment(self, name, ns):
                raise FakeApiException(404)

            def replace_namespaced_deployment(self, name, ns, body):
                calls.append(("replace", name, ns, body["metadata"]["resourceVersion"]))
                return body

            def delete_namespaced_deployment(self, name, ns, body=None):
                calls.append(("delete", name, ns, body))

        api = KubernetesAPI(apps=Apps(), core=object(), custom=object(), coordination=object(),
                            serializer=None, api_exception=FakeApiException)
        self.assertIsNone(api.get("Deployment", "ns", "d"))
        api.update("Deployment", "ns", {"metadata": {"name": "d", "resourceVersion": "7"}})
        api.delete("Deployment", "ns", "d", uid="u1")
        self.assertEqual(calls, [("replace", "d", "ns", "7"),
                                 ("delete", "d", "ns", {"preconditions": {"uid": "u1"}})])


class ChaosHarnessTests(unittest.TestCase):
    SEEDS = range(12)

    def test_v2_converges_with_invariants_on_sample_seeds(self):
        for seed in self.SEEDS:
            r = run_one("v2", make_schedule(seed))
            with self.subTest(seed=seed):
                self.assertTrue(r["converged"])
                self.assertTrue(r["invariants_ok"])
                self.assertEqual(r["unowned_writes"], 0)
                self.assertLessEqual(r["max_simultaneous_leaders"], 1)

    def test_deterministic_for_a_seed(self):
        a, b = run_one("v2", make_schedule(3)), run_one("v2", make_schedule(3))
        self.assertEqual(a, b)

    def test_naive_baseline_is_caught_adopting_squatters(self):
        seeds = [s for s in range(40) if any(f["type"] == "squatter" for f in make_schedule(s).faults)][:3]
        self.assertTrue(seeds)
        self.assertTrue(any(run_one("naive", make_schedule(s))["unowned_writes"] > 0 for s in seeds))
        self.assertTrue(all(run_one("v2", make_schedule(s))["unowned_writes"] == 0 for s in seeds))

    def test_broken_image_schedule_v2_rolls_back_v1_does_not(self):
        seed = next(s for s in range(200) if make_schedule(s).faults[-1]["type"] == "spec_image_broken")
        self.assertTrue(run_one("v2", make_schedule(seed))["converged"])
        self.assertFalse(run_one("v1", make_schedule(seed))["converged"])


if __name__ == "__main__":
    unittest.main()
