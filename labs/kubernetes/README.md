# ☸️ Kubernetes batch execution lab

A real Kubernetes Job runs the shared batch twice and verifies replay stability. The fixture and small scripts are mounted through a ConfigMap; no custom application image or registry account is needed for this exercise.

The Pod runs as a non-root user, has a read-only root filesystem, drops Linux capabilities, has no service-account token, and declares CPU/memory requests and limits. A size-limited `emptyDir` holds temporary outputs. These controls do not constitute a complete security audit.

## Run

Use the `kubernetes` job in [platform CI](../../.github/workflows/platform-ci.yml), which creates an isolated kind cluster. Locally, with Docker, kind and kubectl already available:

```sh
kind create cluster --name portfolio --image kindest/node:v1.32.2
python labs/kubernetes/verify.py
```

The verifier refuses to run unless the current context is `kind-portfolio`, and every Kubernetes command explicitly targets that context. It creates namespace `portfolio-de`, a ConfigMap, an intentionally failing Job and a successful Job. It waits for the actual Failed/Complete conditions and checks application logs.

Inspect with:

```sh
kubectl --context kind-portfolio -n portfolio-de get jobs,pods
kubectl --context kind-portfolio -n portfolio-de logs job/event-batch
```

After inspecting evidence, delete only the disposable cluster:

```sh
kind delete cluster --name portfolio
```

## Boundaries

The failed Job is followed by a separate successful Job; this does not demonstrate automatic retry. `backoffLimit: 0` and `activeDeadlineSeconds` make failure bounded. Replay is inside one Pod; `emptyDir` is not durable across Pod replacement. No service/liveness probe is attached to a finite batch process. No cloud-managed cluster, persistent volume, NetworkPolicy, or cluster operations experience is claimed. Production packaging would normally use a versioned application image rather than source code in a ConfigMap.

Sources: [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/), [kind quickstart](https://kind.sigs.k8s.io/docs/user/quick-start/).
