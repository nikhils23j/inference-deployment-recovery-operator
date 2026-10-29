# Inference Deployment Recovery Operator

**Status:** Personal portfolio project built for the Inferact Cloud Orchestration application on 30 October 2026. It is not employer work and has not served an ML model in production.

The code and evaluation were prepared with Codex for Nikhil's review. Nikhil should study and modify it before presenting it as his own implementation work.

## Problem and design

A one-shot Deployment/Service manifest has no separate controller to restore the objects if they are deleted or drift after installation. This project defines a small `InferenceService` custom resource (image, replicas, port). A Python reconciliation loop lists those resources and compares each desired state with a deterministically named Deployment and Service. Missing children are created; changes to replicas, container image/port, or Service selector/port are patched. Owner references prevent takeover of unrelated objects. A second pass is a no-op.

The loop runs outside the cluster from a kubeconfig and polls every two seconds. It uses Kubernetes API calls through the official Python client, not a mock implementation of the adapter. The sample image is `nginx` for a lightweight local stand-in; this project is about deployment recovery, not inference throughput.

## Reproduce offline checks

Python 3.10+ and the standard library are enough:

```sh
python3 -m unittest -v test_controller.py
python3 demo_offline.py
```

The offline API exercises the same `reconcile()` decisions used by the live adapter across 30 deterministic deletion and drift cases. Its millisecond timings are in-memory function timings and **must not be read as Kubernetes recovery times**. `results-offline.json` records the run.

## Run against a local Kubernetes cluster

Requires an ordinary local cluster (for example, kind or minikube), `kubectl`, and a kubeconfig pointing to that cluster. No cloud account, GPU, or employer resources are needed.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
kubectl apply -f crd.yaml
kubectl wait --for=condition=Established --timeout=60s crd/inferenceservices.portfolio.nikhil.dev
kubectl apply -f sample.yaml
.venv/bin/python k8s_adapter.py --namespace default --interval 2
```

In another terminal, verify `kubectl get deployments,services` and delete or patch the generated `demo-inference` objects. The operator recreates or repairs them on its next pass. Stop the controller with Ctrl+C and delete the generated Deployment to see the one-shot baseline (no custom controller repairs it). The Kubernetes Deployment controller itself still repairs missing Pods; this project tests recovery of the Deployment/Service objects, not Pod restart behavior.

To run the 30-case live evaluation after applying the CRD and sample, stop the foreground operator and run:

```sh
.venv/bin/python evaluate_live.py --namespace default --name demo-inference --output results-live.json
```

The evaluator starts the same reconciliation core in a local thread, injects six fault types five times each against the real API server, times object-spec convergence, and checks a stopped-controller baseline. It restores the sample Deployment at the end. The live evaluator does not measure Pod readiness or service availability.

### Verified local result

On 30 October 2026, the live evaluator ran against a one-node Lima/k3s v1.36.5 cluster on macOS arm64 using Python 3.10.8 and Kubernetes Python client 35.0.0. It repaired **30/30** injected Deployment/Service deletion and drift cases within **0.869 seconds maximum** of each fault; no case exceeded 30 seconds, no controller errors were recorded, and at most two owned objects (one Deployment and one Service) existed. With the custom controller stopped, a deleted Deployment was still absent after three seconds. The separate offline harness also repaired 30/30 deterministic cases, but its timing is not a Kubernetes latency measurement. The live measurement covers **API object-spec convergence**, not Pod readiness or model-serving availability.

The local evaluation used a one-node Lima/k3s cluster on macOS arm64. Set `KUBECONFIG` for your own local cluster before running the commands above.

## Limitations

This small polling controller is single-instance and has no leader election, watch cache, durable status conditions, or production security hardening. The CRD validates a bounded spec but does not perform sophisticated rollout strategy. A live recovery-time target remains unverified unless the local-cluster evaluation is run; offline results support only the reconciliation logic and invariants.
