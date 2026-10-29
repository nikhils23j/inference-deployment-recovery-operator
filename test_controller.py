import unittest

from controller import Desired, reconcile
from offline_api import MemoryAPI


RESOURCE = {
    "apiVersion": "portfolio.nikhil.dev/v1alpha1",
    "kind": "InferenceService",
    "metadata": {"namespace": "default", "name": "demo-inference", "uid": "test-uid-123"},
    "spec": {"image": "nginx:1.27-alpine", "replicas": 2, "port": 80},
}


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.api = MemoryAPI()
        reconcile(self.api, RESOURCE)

    def test_creates_exactly_two_owned_objects_then_noop(self):
        self.assertEqual(len(self.api.objects), 2)
        self.assertEqual(self.api.creates, 2)
        self.assertEqual(reconcile(self.api, RESOURCE), [])
        self.assertEqual(self.api.patches, 0)

    def test_restores_deleted_deployment_and_service(self):
        for kind in ("Deployment", "Service"):
            self.api.delete(kind, "default", "demo-inference")
            self.assertIn(f"created {kind}", reconcile(self.api, RESOURCE))
            self.assertEqual(len(self.api.objects), 2)

    def test_repairs_image_replicas_port_and_selector_drift(self):
        dep = self.api.objects[("Deployment", "default", "demo-inference")]
        svc = self.api.objects[("Service", "default", "demo-inference")]
        dep["spec"]["replicas"] = 9
        dep["spec"]["template"]["spec"]["containers"][0]["image"] = "wrong:latest"
        svc["spec"]["selector"] = {"wrong": "app"}
        svc["spec"]["ports"][0]["port"] = 81
        self.assertEqual(reconcile(self.api, RESOURCE), ["patched Deployment", "patched Service"])
        self.assertEqual(reconcile(self.api, RESOURCE), [])

    def test_refuses_unowned_resource(self):
        self.api.objects[("Deployment", "default", "demo-inference")]["metadata"]["ownerReferences"] = []
        with self.assertRaisesRegex(ValueError, "unowned"):
            reconcile(self.api, RESOURCE)

    def test_validates_bounds(self):
        for spec in ({"image": ""}, {"image": "x", "replicas": 11}, {"image": "x", "port": 0}):
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                Desired.from_resource({"metadata": RESOURCE["metadata"], "spec": spec})


if __name__ == "__main__":
    unittest.main()
