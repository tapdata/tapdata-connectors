#!/usr/bin/env bash
# Run local Paimon S3 tests against an isolated RustFS 1.0.1 container.
set -euo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "Usage: $0 [test-selector] [additional Maven options]"
    echo "Default: PaimonRustFsIntegrationTest; use '*' for the entire connector suite."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_SELECTOR="${1:-PaimonRustFsIntegrationTest}"
if [[ $# -gt 0 ]]; then shift; fi

for dependency in docker curl mvn; do
    command -v "$dependency" >/dev/null || { echo "Missing dependency: $dependency" >&2; exit 1; }
done
MAVEN_JAVA_MAJOR="$(mvn -B -version | sed -n 's/^Java version: \([0-9]*\).*/\1/p')"
if [[ -z "$MAVEN_JAVA_MAJOR" || "$MAVEN_JAVA_MAJOR" -lt 17 ]]; then
    echo "Maven must use JDK 17+ for the Tapdata PDK dependencies; check mvn -version and JAVA_HOME/jEnv." >&2
    exit 1
fi
docker info >/dev/null

# These credentials belong only to this disposable, loopback-only test server.
export PAIMON_S3_ACCESS_KEY="paimon-rustfs-test"
export PAIMON_S3_SECRET_KEY="paimon-rustfs-test-secret"
export PAIMON_RUSTFS_TEST=true
CONTAINER_ID=""
cleanup() {
    if [[ -n "$CONTAINER_ID" ]]; then
        docker rm --force --volumes "$CONTAINER_ID" >/dev/null
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

CONTAINER_ID="$(docker run --detach --rm \
    --publish 127.0.0.1::9000 \
    --volume /data --volume /logs \
    --env RUSTFS_ACCESS_KEY="$PAIMON_S3_ACCESS_KEY" \
    --env RUSTFS_SECRET_KEY="$PAIMON_S3_SECRET_KEY" \
    --env RUSTFS_CONSOLE_ENABLE=false \
    --env RUSTFS_OBS_LOGGER_LEVEL=warn \
    rustfs/rustfs:1.0.1)"
export PAIMON_S3_ENDPOINT="http://$(docker port "$CONTAINER_ID" 9000/tcp)"

READY=false
for ((attempt = 0; attempt < 60; attempt++)); do
    if curl --fail --silent --max-time 2 "$PAIMON_S3_ENDPOINT/health/ready" >/dev/null; then
        READY=true
        break
    fi
    sleep 1
done
if [[ "$READY" != true ]]; then
    docker logs --tail 80 "$CONTAINER_ID" >&2
    echo "RustFS did not become ready: $PAIMON_S3_ENDPOINT" >&2
    exit 1
fi

echo "Testing Paimon with rustfs/rustfs:1.0.1 at $PAIMON_S3_ENDPOINT"
mvn -B -ntp -f "$PROJECT_ROOT/connectors/paimon-plus-connector/pom.xml" \
    -Dtest="$TEST_SELECTOR" test "$@"
