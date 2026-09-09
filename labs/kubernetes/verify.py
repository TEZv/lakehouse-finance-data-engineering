"""Only mutates the dedicated local kind-portfolio context."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def kubectl(*args, input=None):
    return subprocess.run(["kubectl", "--context", "kind-portfolio", *args],
                          input=input, text=True, capture_output=True, check=True).stdout


assert subprocess.check_output(["kubectl", "config", "current-context"], text=True).strip() == "kind-portfolio"
namespace = kubectl("create", "namespace", "portfolio-de", "--dry-run=client", "-o", "json")
kubectl("apply", "-f", "-", input=namespace)
config = kubectl("-n", "portfolio-de", "create", "configmap", "event-batch-code",
                 "--from-file=" + str(ROOT / "labs/platform/batch.py"),
                 "--from-file=" + str(ROOT / "labs/platform/events.json"),
                 "--from-file=" + str(ROOT / "labs/kafka/event_sink.py"),
                 "--from-file=" + str(ROOT / "labs/kubernetes/run_batch.py"),
                 "--dry-run=client", "-o", "json")
kubectl("apply", "-f", "-", input=config)
template = json.loads(kubectl("create", "-f", str(ROOT / "labs/kubernetes/job.yaml"),
                              "--dry-run=client", "-o", "json"))
for name, fail in [("event-batch-failure", True), ("event-batch", False)]:
    job = json.loads(json.dumps(template))
    job["metadata"]["name"] = name
    for item in job["spec"]["template"]["spec"]["containers"][0]["env"]:
        if item["name"] == "INJECT_FAILURE":
            item["value"] = "1" if fail else "0"
    kubectl("apply", "-f", "-", input=json.dumps(job))
    kubectl("-n", "portfolio-de", "wait", "--for=condition=" + ("failed" if fail else "complete"),
            "job/" + name, "--timeout=180s")
    logs = kubectl("-n", "portfolio-de", "logs", "job/" + name)
    assert ("Intentional failure" if fail else "PASS: Kubernetes batch") in logs, logs
    print(logs)
print("PASS: real Kubernetes Job failure observed, successful Job, restricted Pod, replay")
