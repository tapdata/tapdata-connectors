# ClickHouse IT fixture

This fixture provides the default JSON source used by the ClickHouse connector IT.
It deliberately uses the Kubernetes Service DNS name rather than `127.0.0.1`, because
the GitHub runner executes in a separate Pod.

The `taptest-dbs` namespace must already exist. The fixture is intended for the shared k3s
IT cluster; create it first in a new cluster with:

```bash
kubectl create namespace taptest-dbs --dry-run=client -o yaml | kubectl apply -f -
```

```bash
kubectl apply -f deploy/clickhouse/clickhouse-23.7.yaml
kubectl rollout status deployment/clickhouse -n taptest-dbs --timeout=5m
kubectl run clickhouse-ping --rm -i --restart=Never -n tapdata-it \
  --image=curlimages/curl:8.10.1 -- curl -fsS http://clickhouse.taptest-dbs.svc.cluster.local:8123/ping
```

If the runner cannot create temporary Pods, run the same `curl` command from an existing
runner Pod instead.

The DBForge source is intentionally separate. It requests `clickhouse/dedicated/single`
and provisions an isolated temporary instance through DBForge instead of using this fixture.
