"""Real Kubernetes client adapter; loaded only for live cluster use."""
from __future__ import annotations

import argparse
import logging
import time

from controller import GROUP, PLURAL, VERSION, reconcile


class KubernetesAPI:
    def __init__(self):
        from kubernetes import client, config
        from kubernetes.client.rest import ApiException

        config.load_kube_config()
        self.client = client
        self.ApiException = ApiException
        self.apps = client.AppsV1Api()
        self.core = client.CoreV1Api()
        self.custom = client.CustomObjectsApi()
        self.serializer = client.ApiClient()

    def resources(self, namespace: str) -> list[dict]:
        response = self.custom.list_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL)
        return response.get("items", [])

    def get(self, kind: str, namespace: str, name: str) -> dict | None:
        try:
            if kind == "Deployment":
                obj = self.apps.read_namespaced_deployment(name, namespace)
            else:
                obj = self.core.read_namespaced_service(name, namespace)
            return self.serializer.sanitize_for_serialization(obj)
        except self.ApiException as exc:
            if exc.status == 404:
                return None
            raise

    def create(self, kind: str, namespace: str, body: dict) -> None:
        try:
            if kind == "Deployment":
                self.apps.create_namespaced_deployment(namespace, body)
            else:
                self.core.create_namespaced_service(namespace, body)
        except self.ApiException as exc:
            # Another reconcile may have created the deterministic name. Re-read next pass.
            if exc.status != 409:
                raise

    def patch(self, kind: str, namespace: str, name: str, body: dict) -> None:
        if kind == "Deployment":
            operations = [
                {"op": "replace", "path": "/spec/replicas", "value": body["spec"]["replicas"]},
                {"op": "replace", "path": "/spec/template", "value": body["spec"]["template"]},
            ]
            self.apps.patch_namespaced_deployment(name, namespace, operations)
        else:
            spec = body["spec"]
            selector = {key: value for key, value in spec["selector"].items() if value is not None}
            operations = [
                {"op": "replace", "path": "/spec/selector", "value": selector},
                {"op": "replace", "path": "/spec/ports", "value": spec["ports"]},
            ]
            self.core.patch_namespaced_service(name, namespace, operations)


def run(namespace: str, interval: float, once: bool = False) -> None:
    api = KubernetesAPI()
    while True:
        try:
            for resource in api.resources(namespace):
                try:
                    actions = reconcile(api, resource)
                    for action in actions:
                        logging.info("%s/%s: %s", namespace, resource["metadata"]["name"], action)
                except Exception:
                    logging.exception("reconcile failed for %s", resource.get("metadata", {}).get("name"))
        except Exception:
            logging.exception("custom-resource listing failed")
        if once:
            return
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args.namespace, args.interval, args.once)
