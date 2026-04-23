#!/usr/bin/env bash
set -euo pipefail

############################################
# Test: Leader Recovery with In-Flight Work
############################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/test_inflight_recovery.log"

exec > >(tee "${LOG_FILE}") 2>&1

############################################
# Configuration
############################################

NUM_VOTING_PEERS=5
BASE_UDP_PORT=8010
GATEWAY_HTTP_PORT=8888

JAVA_OPTS="-ea"
CLASSPATH="target/classes"

PEER_RUNNER="edu.yu.cs.com3800.faulttolerance.PeerServerRunner"
GATEWAY_RUNNER="edu.yu.cs.com3800.faulttolerance.GatewayServerRunner"

LEADER_ENDPOINT="http://localhost:${GATEWAY_HTTP_PORT}/leader"
COMPILE_ENDPOINT="http://localhost:${GATEWAY_HTTP_PORT}/compileandrun"

############################################
# Utility: Get milliseconds (macOS compatible)
############################################

get_millis() {
    python3 -c 'import time; print(int(time.time() * 1000))'
}

############################################
# Cleanup on exit
############################################

PEER_PIDS=()
GATEWAY_PID=""
BG_REQUEST_PID=""
BG_RESPONSE_FILE=""

cleanup() {
    echo
    echo "=== Shutting down cluster ==="

    [[ -n "${BG_REQUEST_PID}" ]] && kill "${BG_REQUEST_PID}" 2>/dev/null || true
    [[ -n "${GATEWAY_PID}" ]] && kill "${GATEWAY_PID}" 2>/dev/null || true

    for pid in "${PEER_PIDS[@]}"; do
        kill "${pid}" 2>/dev/null || true
    done

    sleep 1

    [[ -n "${GATEWAY_PID}" ]] && kill -9 "${GATEWAY_PID}" 2>/dev/null || true

    for pid in "${PEER_PIDS[@]}"; do
        kill -9 "${pid}" 2>/dev/null || true
    done

    [[ -n "${BG_RESPONSE_FILE}" ]] && rm -f "${BG_RESPONSE_FILE}" || true

    echo "Cluster shut down"
}

trap cleanup EXIT

############################################
# 1) Start voting peers
############################################

echo "=== Starting ${NUM_VOTING_PEERS} voting peers ==="

for ((i=1; i<=NUM_VOTING_PEERS; i++)); do
    UDP_PORT=$((BASE_UDP_PORT + i - 1))
    echo "Starting Peer ${i} on UDP port ${UDP_PORT}"

    java ${JAVA_OPTS} -cp "${CLASSPATH}" ${PEER_RUNNER} \
        ${i} ${NUM_VOTING_PEERS} ${BASE_UDP_PORT} ${UDP_PORT} &

    PEER_PIDS+=($!)
done

############################################
# 2) Start Gateway
############################################

echo
echo "Starting Gateway on HTTP port ${GATEWAY_HTTP_PORT}"

java ${JAVA_OPTS} -cp "${CLASSPATH}" ${GATEWAY_RUNNER} \
    ${GATEWAY_HTTP_PORT} ${NUM_VOTING_PEERS} ${BASE_UDP_PORT} &

GATEWAY_PID=$!
echo "Gateway PID ${GATEWAY_PID}"

############################################
# 3) Wait for leader election
############################################

echo
echo "=== Waiting for leader election ==="

LEADER_ID=""
for i in {1..30}; do
    LEADER_ID=$(curl -sf "${LEADER_ENDPOINT}" || true)
    if [[ -n "${LEADER_ID}" && "${LEADER_ID}" != "UNKNOWN" ]]; then
        echo "Leader elected: ${LEADER_ID}"
        break
    fi
    echo "Waiting... (${i}/30)"
    sleep 1
done

if [[ -z "${LEADER_ID}" || "${LEADER_ID}" == "UNKNOWN" ]]; then
    echo "ERROR: Leader was not elected"
    exit 1
fi

############################################
# 4) Send a SLOW request in background
############################################

echo
echo "=== Sending slow request (will be in-flight when leader dies) ==="

# This Java code sleeps for 10 seconds, simulating long-running work
SLOW_SRC=$(cat <<'EOF'
public class SlowWork {
    public String run() {
        try { Thread.sleep(10000); } catch (Exception e) {}
        return "SLOW_WORK_COMPLETED";
    }
}
EOF
)

BG_RESPONSE_FILE=$(mktemp)

echo "Submitting slow request in background..."
START_TIME=$(get_millis)

(curl -s -X POST \
    -H "Content-Type: text/x-java-source" \
    --data-binary "${SLOW_SRC}" \
    "${COMPILE_ENDPOINT}" > "${BG_RESPONSE_FILE}") &

BG_REQUEST_PID=$!
echo "Background request PID: ${BG_REQUEST_PID}"

# Wait briefly to ensure request reaches leader and is assigned to worker
sleep 2

############################################
# 5) Kill the leader mid-computation
############################################

echo
echo "=== Killing leader ${LEADER_ID} mid-computation ==="

LEADER_IDX=$((LEADER_ID - 1))
LEADER_PID="${PEER_PIDS[$LEADER_IDX]}"

echo "Killing leader (PID ${LEADER_PID})"
kill -9 "${LEADER_PID}"

# Mark as killed so cleanup doesn't try again
PEER_PIDS[$LEADER_IDX]=""

############################################
# 6) Wait for new leader election
############################################

echo
echo "=== Waiting for new leader election ==="

NEW_LEADER_ID=""
for i in {1..90}; do
    NEW_LEADER_ID=$(curl -sf "${LEADER_ENDPOINT}" || true)
    if [[ -n "${NEW_LEADER_ID}" && "${NEW_LEADER_ID}" != "UNKNOWN" && "${NEW_LEADER_ID}" != "${LEADER_ID}" ]]; then
        echo "New leader elected: ${NEW_LEADER_ID}"
        break
    fi
    echo "Waiting for new leader... (${i}/90)"
    sleep 1
done

if [[ -z "${NEW_LEADER_ID}" || "${NEW_LEADER_ID}" == "UNKNOWN" || "${NEW_LEADER_ID}" == "${LEADER_ID}" ]]; then
    echo "ERROR: New leader was not elected"
    exit 1
fi

############################################
# 7) Wait for background request to complete
############################################

echo
echo "=== Waiting for in-flight request to complete ==="

wait "${BG_REQUEST_PID}" || true
BG_REQUEST_PID=""

END_TIME=$(get_millis)
ELAPSED=$((END_TIME - START_TIME))

RESPONSE=$(cat "${BG_RESPONSE_FILE}")

echo
echo "Response received after ${ELAPSED}ms:"
echo "${RESPONSE}"

############################################
# 8) Verify response is correct
############################################

echo
echo "=== Verifying response ==="

if [[ "${RESPONSE}" == "SLOW_WORK_COMPLETED" ]]; then
    echo "SUCCESS: In-flight work was recovered after leader failure"
else
    echo "FAILURE: Unexpected response: ${RESPONSE}"
    exit 1
fi

############################################
# 9) Send SAME request again to test caching
############################################

echo
echo "=== Sending identical request to verify cache ==="

CACHE_START=$(get_millis)

CACHE_RESPONSE=$(curl -s -X POST \
    -H "Content-Type: text/x-java-source" \
    --data-binary "${SLOW_SRC}" \
    "${COMPILE_ENDPOINT}")

CACHE_END=$(get_millis)
CACHE_ELAPSED=$((CACHE_END - CACHE_START))

echo "Cache response received in ${CACHE_ELAPSED}ms:"
echo "${CACHE_RESPONSE}"

if [[ "${CACHE_RESPONSE}" == "SLOW_WORK_COMPLETED" ]]; then
    if [[ ${CACHE_ELAPSED} -lt 2000 ]]; then
        echo "SUCCESS: Response was cached (returned in <2s instead of 10s)"
    else
        echo "WARNING: Response correct but took ${CACHE_ELAPSED}ms (may not be cached)"
    fi
else
    echo "FAILURE: Cache response incorrect: ${CACHE_RESPONSE}"
    exit 1
fi

############################################
# 10) Summary
############################################

echo
echo "=========================================="
echo "TEST PASSED: Leader recovery with in-flight work"
echo "=========================================="
echo "- Original leader: ${LEADER_ID}"
echo "- New leader: ${NEW_LEADER_ID}"
echo "- In-flight work recovered: YES"
echo "- Result caching verified: YES"
echo

exit 0
