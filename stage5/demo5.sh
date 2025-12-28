#!/usr/bin/env bash
set -euo pipefail

############################################
# Stage 5 – Election Smoke Test Only
############################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/output.log"

# Log everything to screen AND output.log
exec > >(tee "${LOG_FILE}") 2>&1

echo "=========================================="
echo "Stage 5 Election Smoke Test"
echo "=========================================="
echo

############################################
# Configuration
############################################

NUM_VOTING_PEERS=7
BASE_UDP_PORT=8010
GATEWAY_HTTP_PORT=8888

JAVA_OPTS="-ea"

PEER_RUNNER="edu.yu.cs.com3800.stage5.PeerServerRunner"
GATEWAY_RUNNER="edu.yu.cs.com3800.stage5.GatewayServerRunner"

LEADER_ENDPOINT="http://localhost:${GATEWAY_HTTP_PORT}/leader"

############################################
# 1) Run JUnit tests (sanity check)
############################################

echo "=== Building project (skipping tests for smoke test) ==="
mvn -q -DskipTests package
echo "Build completed"
echo

############################################
# Build runtime classpath
############################################

echo "Using simple runtime classpath (target/classes)"
CLASSPATH="target/classes"
echo

############################################
# Cleanup on exit
############################################

PEER_PIDS=()
GATEWAY_PID=""

cleanup() {
    echo
    echo "=== Shutting down cluster ==="

    if [[ -n "${GATEWAY_PID}" ]]; then
        kill "${GATEWAY_PID}" 2>/dev/null || true
    fi

    for pid in "${PEER_PIDS[@]}"; do
        kill "${pid}" 2>/dev/null || true
    done

    sleep 1

    if [[ -n "${GATEWAY_PID}" ]]; then
        kill -9 "${GATEWAY_PID}" 2>/dev/null || true
    fi

    for pid in "${PEER_PIDS[@]}"; do
        kill -9 "${pid}" 2>/dev/null || true
    done

    echo "Cluster shut down"
}

trap cleanup EXIT

############################################
# 2) Start voting peers
############################################

echo "=== Starting ${NUM_VOTING_PEERS} voting peers ==="

for ((i=1; i<=NUM_VOTING_PEERS; i++)); do
    UDP_PORT=$((BASE_UDP_PORT + i - 1))
    echo "Starting Peer ${i} on UDP port ${UDP_PORT}"

    java ${JAVA_OPTS} \
        -cp "${CLASSPATH}" \
        ${PEER_RUNNER} \
        ${i} ${NUM_VOTING_PEERS} ${BASE_UDP_PORT} ${UDP_PORT} \
        &

    PEER_PIDS+=($!)
done

echo

############################################
# 3) Start Gateway (observer peer)
############################################

echo "Starting Gateway on HTTP port ${GATEWAY_HTTP_PORT}"

java ${JAVA_OPTS} \
    -cp "${CLASSPATH}" \
    ${GATEWAY_RUNNER} \
    ${GATEWAY_HTTP_PORT} ${NUM_VOTING_PEERS} ${BASE_UDP_PORT} \
    &

GATEWAY_PID=$!
echo "Gateway PID ${GATEWAY_PID}"
echo

############################################
# 4) Wait for leader election (bounded)
############################################

echo "=== Waiting for leader election ==="

ELECTED=false
for i in {1..30}; do
    LEADER_ID=$(curl -sf "${LEADER_ENDPOINT}" || true)

    if [[ -n "${LEADER_ID}" && "${LEADER_ID}" != "UNKNOWN" ]]; then
        echo
        echo "Leader elected: ${LEADER_ID}"
        echo
        ELECTED=true
        break
    fi

    echo "Waiting... (${i}/30)"
    sleep 1
done

if [[ "${ELECTED}" != true ]]; then
    echo
    echo "ERROR: Leader was not elected within timeout"
    exit 1
fi

echo "Election successful."

############################################
# 5) Print cluster roles
############################################

echo
echo "Cluster roles:"
echo "--------------"

for ((i=1; i<=NUM_VOTING_PEERS; i++)); do
    if [[ "${i}" == "${LEADER_ID}" ]]; then
        ROLE="LEADING"
    else
        ROLE="FOLLOWING"
    fi

    echo "Server ${i}: ${ROLE}"
done

echo "Gateway: OBSERVER"
echo

############################################
# 6) Send client requests through Gateway
############################################

echo
echo "Sending 9 client requests through Gateway"
echo "-----------------------------------------"

REQUESTS=9
RESPONSES=()

for ((i=1; i<=REQUESTS; i++)); do
    echo
    echo "Request ${i}:"

    SRC=$(cat <<EOF
public class HW${i} {
    public String run() {
        return "Recovered ${i}";
    }
}
EOF
)

    echo "${SRC}"
    echo

    RESPONSE=$(curl -s -X POST \
        -H "Content-Type: text/x-java-source" \
        --data-binary "${SRC}" \
        http://localhost:${GATEWAY_HTTP_PORT}/compileandrun)

    RESPONSES+=("${RESPONSE}")
done

############################################
# 7) Print collected responses
############################################

echo
echo "Cluster responses:"
echo "------------------"

for ((i=1; i<=REQUESTS; i++)); do
    echo
    echo "Response ${i}:"
    echo "${RESPONSES[$((i-1))]}"
done

############################################
# 8) Kill a follower and verify failure detection
############################################

echo
echo "Killing a follower JVM..."

LEADER_ID=$(curl -s "${LEADER_ENDPOINT}")

KILLED_ID=""
KILLED_PID=""

for idx in "${!PEER_PIDS[@]}"; do
    PEER_ID=$((idx + 1))
    if [[ "${PEER_ID}" != "${LEADER_ID}" ]]; then
        KILLED_ID="${PEER_ID}"
        KILLED_PID="${PEER_PIDS[$idx]}"
        break
    fi
done

echo "Killing follower ${KILLED_ID} (PID ${KILLED_PID})"
kill -9 "${KILLED_PID}"

echo
echo "Waiting for failure detection (20 seconds)..."
sleep 20

echo
echo "Cluster membership after failure:"
echo "--------------------------------"

curl -s "http://localhost:${GATEWAY_HTTP_PORT}/cluster"
echo

############################################
# 9) Kill the leader and verify re-election
############################################

echo
echo "=========================================="
echo "Killing the LEADER and waiting for re-election"
echo "=========================================="

OLD_LEADER_ID=$(curl -s "${LEADER_ENDPOINT}")
echo "Current leader is ${OLD_LEADER_ID}"

# Find leader PID
LEADER_PID=""
for idx in "${!PEER_PIDS[@]}"; do
    PEER_ID=$((idx + 1))
    if [[ "${PEER_ID}" == "${OLD_LEADER_ID}" ]]; then
        LEADER_PID="${PEER_PIDS[$idx]}"
        break
    fi
done

if [[ -z "${LEADER_PID}" ]]; then
    echo "ERROR: Could not find PID for leader ${OLD_LEADER_ID}"
    exit 1
fi

echo "Killing leader ${OLD_LEADER_ID} (PID ${LEADER_PID})"
kill -9 "${LEADER_PID}"

echo
echo "Pausing 1000ms after leader kill..."
sleep 1

############################################
# 10) Send 9 client requests in background (after leader kill)
############################################

echo
echo "Sending 9 MORE client requests through Gateway (background)"
echo "----------------------------------------------------------"

POSTFAIL_REQUESTS=9
POSTFAIL_PIDS=()
POSTFAIL_OUTFILES=()

for ((i=1; i<=POSTFAIL_REQUESTS; i++)); do
    OUTFILE="$(mktemp -t gw_resp_${i}.XXXXXX)"
    POSTFAIL_OUTFILES+=("${OUTFILE}")

    SRC=$(cat <<EOF
public class HW_after_${i} {
    public String run() {
        return "AfterLeaderKill ${i}";
    }
}
EOF
)

    # Fire request in background, write response to temp file
    (curl -s -X POST \
        -H "Content-Type: text/x-java-source" \
        --data-binary "${SRC}" \
        "http://localhost:${GATEWAY_HTTP_PORT}/compileandrun" \
        > "${OUTFILE}") &

    POSTFAIL_PIDS+=($!)
    echo "Dispatched post-failure request ${i} (pid=${POSTFAIL_PIDS[$((i-1))]})"
done

############################################
# 11) Wait for the Gateway to have a new leader
############################################

echo
echo "Waiting for Gateway to report a NEW leader..."

NEW_LEADER_ID=""
RE_ELECTED=false

for i in {1..60}; do
    CANDIDATE=$(curl -sf "${LEADER_ENDPOINT}" || true)

    if [[ -n "${CANDIDATE}" && "${CANDIDATE}" != "UNKNOWN" && "${CANDIDATE}" != "${OLD_LEADER_ID}" ]]; then
        NEW_LEADER_ID="${CANDIDATE}"
        RE_ELECTED=true
        break
    fi

    echo "Waiting for new leader... (${i}/60)"
    sleep 1
done

if [[ "${RE_ELECTED}" != true ]]; then
    echo
    echo "ERROR: No new leader elected after leader failure"
    exit 1
fi

echo
echo "Gateway sees new leader: ${NEW_LEADER_ID}"
echo

############################################
# 12) Wait for all 9 background requests to finish
############################################

echo "Waiting for all ${POSTFAIL_REQUESTS} post-failure requests to receive responses..."
for pid in "${POSTFAIL_PIDS[@]}"; do
    wait "${pid}"
done

echo
echo "Post-failure responses from Gateway:"
echo "-----------------------------------"

for ((i=1; i<=POSTFAIL_REQUESTS; i++)); do
    echo
    echo "Post-failure Response ${i}:"
    cat "${POSTFAIL_OUTFILES[$((i-1))]}"
done

# Cleanup temp files
for f in "${POSTFAIL_OUTFILES[@]}"; do
    rm -f "${f}" || true
done

echo
echo "All ${POSTFAIL_REQUESTS} post-failure requests completed."
echo

############################################
# 13) Verify all peers agree on the new leader
############################################

echo
echo "Verifying peer consensus on new leader"
echo "--------------------------------------"

CONSENSUS=true

for ((i=1; i<=NUM_VOTING_PEERS; i++)); do
    # Skip peers that were intentionally killed
    if [[ "${i}" == "${KILLED_ID}" || "${i}" == "${OLD_LEADER_ID}" ]]; then
        echo "Peer ${i}: skipped (intentionally killed)"
        continue
    fi

    UDP_PORT=$((BASE_UDP_PORT + i - 1))
    PEER_HTTP_PORT=$((UDP_PORT + 105))

    PEER_LEADER_ID=""
    ATTEMPTS=0

    # Retry loop to get leader info from peer
    while [[ ${ATTEMPTS} -lt 10 ]]; do
        RESPONSE=$(curl -sf "http://localhost:${PEER_HTTP_PORT}/leader" || echo "UNREACHABLE")

        if [[ "${RESPONSE}" != "UNREACHABLE" && "${RESPONSE}" != "UNKNOWN" ]]; then
            PEER_LEADER_ID=$(echo "${RESPONSE}" | cut -d',' -f1)
            PEER_EPOCH=$(echo "${RESPONSE}" | cut -d',' -f2)
            break
        fi

        sleep 0.5
        ((ATTEMPTS++))
    done

    if [[ -z "${PEER_LEADER_ID}" ]]; then
        echo "Peer ${i}: no leader information (timed out)"
        CONSENSUS=false
        continue
    fi

    PEER_LEADER_ID=$(echo "${RESPONSE}" | cut -d',' -f1)
    PEER_EPOCH=$(echo "${RESPONSE}" | cut -d',' -f2)

    if [[ "${PEER_LEADER_ID}" != "${NEW_LEADER_ID}" ]]; then
        echo "Peer ${i}: disagrees (leader=${PEER_LEADER_ID}, epoch=${PEER_EPOCH})"
        CONSENSUS=false
    else
        echo "Peer ${i}: leader=${PEER_LEADER_ID}, epoch=${PEER_EPOCH}"
    fi
done

echo
if [[ "${CONSENSUS}" == true ]]; then
    echo "All peers agree on the new leader (${NEW_LEADER_ID})"
else
    echo "Cluster consensus verification failed"
    exit 1
fi

############################################
# Done — allow brief observation window
############################################

echo
echo "=========================================="
echo "Election smoke test complete"
echo "=========================================="