# COM3800 Distributed Compute Cluster

A five-stage Java distributed-systems project that evolves from a single-node compile-and-run HTTP server into a fault-tolerant cluster with leader election, a gateway/observer node, request caching, gossip-based failure detection, and recovery of in-flight work.

This repository is organized by capability so the progression is easy to follow:

- `compile-run-server`: single-node HTTP server that compiles Java source sent to `/compileandrun` and executes its `run()` method.
- `leader-election-cluster`: UDP-based peer discovery and leader election with quorum handling.
- `distributed-work-routing`: distributed work execution with a leader delegating requests to followers.
- `gateway-observer-cluster`: gateway/observer architecture, TCP work forwarding, HTTP entrypoint, and cached responses.
- `fault-tolerant-cluster`: gossip heartbeats, failure detection, leader failover, worker failure reassignment, and recovery of in-flight requests.

## Highlights

- HTTP API for submitting Java source with content type `text/x-java-source`
- Leader election across peer servers
- Observer gateway that never votes but tracks the current leader
- Round-robin work dispatch from leader to workers
- Result caching for identical requests
- Gossip-based heartbeat propagation and failed-node detection
- Re-election after leader failure
- Recovery when a worker or leader fails while work is in flight

## Tech Stack

- Java 21
- Maven
- JUnit 5
- `com.sun.net.httpserver.HttpServer` for the HTTP endpoints
- UDP for election/gossip traffic
- TCP for work distribution between gateway, leader, and workers

## Repository Layout

```text
distributed-serverless-java-cluster/
├── compile-run-server/
├── leader-election-cluster/
├── distributed-work-routing/
├── gateway-observer-cluster/
└── fault-tolerant-cluster/
```

Each stage is self-contained with its own `pom.xml`, source tree, and tests.

## Fault-Tolerant Cluster Architecture

The final stage is the most complete version of the system:

1. Clients send Java source to the gateway at `/compileandrun`.
2. The gateway validates the request, checks its cache, and forwards cache misses to the current leader.
3. The leader distributes work to follower nodes over TCP.
4. Followers compile and run the submitted Java code, then return the result.
5. The gateway serves the response and caches it for repeated requests.
6. Gossip and heartbeat threads continuously detect failed peers.
7. If a worker or leader dies, the cluster reassigns or recovers pending work and elects a new leader when needed.

Useful final-stage endpoints:

- `POST /compileandrun`: submit Java source
- `GET /leader`: ask the gateway which leader it currently sees
- `GET /cluster`: inspect leader and currently live nodes
- `GET /status`: per-peer status endpoint exposed by `PeerServerRunner`

## Running A Stage

Each module can be built independently. For example, to build and test the final fault-tolerant cluster:

```bash
cd fault-tolerant-cluster
mvn test
```

If you want to start the final cluster manually:

```bash
cd fault-tolerant-cluster
mvn clean compile
java -cp target/classes edu.yu.cs.com3800.faulttolerance.PeerServerRunner 1 5 8010 8010
java -cp target/classes edu.yu.cs.com3800.faulttolerance.PeerServerRunner 2 5 8010 8011
java -cp target/classes edu.yu.cs.com3800.faulttolerance.PeerServerRunner 3 5 8010 8012
java -cp target/classes edu.yu.cs.com3800.faulttolerance.PeerServerRunner 4 5 8010 8013
java -cp target/classes edu.yu.cs.com3800.faulttolerance.PeerServerRunner 5 5 8010 8014
java -cp target/classes edu.yu.cs.com3800.faulttolerance.GatewayServerRunner 8888 5 8010
```

Then submit a request:

```bash
curl -X POST \
  -H "Content-Type: text/x-java-source" \
  --data-binary 'public class HW { public String run() { return "Hello cluster"; } }' \
  http://localhost:8888/compileandrun
```

## Demo Scripts And Tests

The repository includes both JUnit tests and shell demos, especially in `fault-tolerant-cluster`:

- `demo5.sh`: brings up a larger cluster, exercises request handling, kills a follower, kills the leader, and verifies re-election.
- `test_inflight_recovery.sh`: validates that a long-running request still completes after a leader failure.
- `WorkerFailureReassignmentTest`: verifies that killing a worker mid-flight does not lose the client request.
- `GatewayServerTest` / `NewGatewayServerTest`: validate compile-and-run behavior, caching, concurrency, and observer behavior.

## Publishing Notes

Before making this repository public, it is worth cleaning a few items that are local or school-specific:

- Remove generated files such as `.DS_Store`, `target/`, and log output from version control if they are currently tracked.
- If the original school repository contained starter code or policy-restricted material, confirm that your class allows public publication.

## What This Project Demonstrates

This project shows incremental construction of a distributed system:

- request validation and dynamic Java execution
- leader election and quorum logic
- multi-node request routing
- observer/gateway design
- caching and concurrency control
- heartbeat gossip and failure detection
- failover and recovery under partial node failure

## Author

Jacob Neuer
