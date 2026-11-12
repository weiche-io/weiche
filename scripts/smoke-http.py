#!/usr/bin/env python3
"""Exercise the local demo through its real compio listener and Kubernetes watches."""
import http.client
import json
import os
import subprocess
import sys
import time
from urllib.parse import urlsplit

url = urlsplit(sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:18080")


def request(path="/api/hello?x=1", host="echo.demo.local", method="GET", body=None, headers=None):
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
    connection.request(method, path, body, {"Host": host, **(headers or {})})
    response = connection.getresponse()
    payload = response.read()
    result = response.status, payload, dict(response.getheaders())
    connection.close()
    return result


def eventually(check, seconds=30):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, AssertionError, http.client.HTTPException) as error:
            last = error
        time.sleep(0.2)
    raise AssertionError(f"condition failed to converge: {last}")


def kubectl(*args):
    # A dedicated kubeconfig and context are required for all mutations.
    assert os.environ.get("KUBECONFIG"), "KUBECONFIG must point to the local demo"
    command = ["kubectl", "--context", "kind-weiche-demo", "-n", "weiche-demo", *args]
    return subprocess.check_output(command, text=True)


def patch_route(operations):
    kubectl("patch", "httproute", "echo", "--type=json", "-p", json.dumps(operations))


eventually(lambda: request()[0] == 200)
status, payload, headers = request()
data = json.loads(payload)
assert status == 200 and data["backend"] == "echo-a" and data["path"] == "/api/hello?x=1"
assert data["host"] == "echo.demo.local"
assert "x-backend-hop" not in {key.lower() for key in headers}
assert request("/apix")[0] == 404 and request(host="wrong.demo.local")[0] == 404
assert json.loads(request("/api/special")[1])["backend"] == "echo-b"
assert json.loads(request("/api/special/child")[1])["backend"] == "echo-a"
payload = "流式 HTTP demo" * 1000
status, echoed, _ = request(method="POST", body=payload.encode(), headers={"Connection": "x-remove", "x-remove": "secret"})
assert status == 200 and json.loads(echoed)["body"] == payload
assert "x-remove" not in {key.lower() for key in json.loads(echoed)["headers"]}
# An iterable body forces chunked request framing.
assert json.loads(request(method="POST", body=iter([b"hello", b" world"]))[1])["body"] == "hello world"
pods = {json.loads(request()[1])["pod"] for _ in range(8)}
assert len(pods) == 2, pods
# The first SSE frame must arrive before the delayed final frame.
connection = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
start = time.monotonic()
connection.request("GET", "/api/events", headers={"Host": "echo.demo.local"})
response = connection.getresponse()
assert response.status == 200 and response.readline() == b"data: 0\n"
first = time.monotonic() - start
remaining = response.read()
elapsed = time.monotonic() - start
assert b"data: 2" in remaining and first < 0.9 and elapsed >= 1, (first, elapsed)
connection.close()
# Reuse one downstream keep-alive connection.
connection = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
for path in ["/api/one", "/api/two"]:
    connection.request("GET", path, headers={"Host": "echo.demo.local"})
    response = connection.getresponse()
    assert response.status == 200 and json.loads(response.read())["path"] == path
connection.close()
print(f"PASS HTTP routing, precedence, POST/chunked upload, hop headers, keep-alive, two endpoints, SSE (first {first:.3f}s / total {elapsed:.3f}s)")

backend_path = "/spec/rules/0/backendRefs/0/name"
try:
    patch_route([{"op": "replace", "path": backend_path, "value": "echo-b"}])
    eventually(lambda: json.loads(request()[1])["backend"] == "echo-b")
    patch_route([{"op": "replace", "path": backend_path, "value": "missing"}])
    eventually(lambda: request()[0] == 500)
    def unresolved():
        route = json.loads(kubectl("get", "httproute", "echo", "-o", "json"))
        return any(c["type"] == "ResolvedRefs" and c["status"] == "False" and c["observedGeneration"] == route["metadata"]["generation"]
                   for p in route.get("status", {}).get("parents", []) for c in p["conditions"])
    eventually(unresolved)
finally:
    patch_route([{"op": "replace", "path": backend_path, "value": "echo-a"}])
eventually(lambda: request()[0] == 200)
try:
    kubectl("scale", "deployment", "echo-a", "--replicas=0")
    eventually(lambda: request()[0] == 503)
finally:
    kubectl("scale", "deployment", "echo-a", "--replicas=2")
eventually(lambda: request()[0] == 200, seconds=60)
try:
    patch_route([{"op": "replace", "path": "/spec/parentRefs/0/sectionName", "value": "missing"}])
    eventually(lambda: request()[0] == 404)
finally:
    patch_route([{"op": "replace", "path": "/spec/parentRefs/0/sectionName", "value": "http"}])
eventually(lambda: request()[0] == 200)
print("PASS watch hot update, invalid backend + status, endpoint removal/recovery, parent detach/reattach")


def statuses_ready():
    route = json.loads(kubectl("get", "httproute", "echo", "-o", "json"))
    gateway = json.loads(kubectl("get", "gateway", "weiche", "-o", "json"))
    route_conditions = [c for p in route.get("status", {}).get("parents", []) for c in p["conditions"]]
    return (len(route_conditions) == 2
            and all(c["status"] == "True" and c["observedGeneration"] == route["metadata"]["generation"] for c in route_conditions)
            and any(c["type"] == "Programmed" and c["status"] == "True" for c in gateway["status"]["conditions"]))


eventually(statuses_ready)


def status_versions():
    return tuple(json.loads(kubectl("get", kind, name, "-o", "json"))["metadata"]["resourceVersion"]
                 for kind, name in [("gatewayclass", "weiche"), ("gateway", "weiche"), ("httproute", "echo")])


before = status_versions()
time.sleep(1.2)
assert before == status_versions(), "unchanged resources must not produce a status update loop"
print("PASS current observedGeneration, Programmed status, stable resourceVersions")
