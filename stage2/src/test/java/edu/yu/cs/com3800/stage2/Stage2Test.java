package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.PeerServer.ServerState;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.stage2.PeerServerImpl;

import org.junit.jupiter.api.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

public class Stage2Test {

    private static final Logger LOG = Logger.getLogger(Stage2Test.class.getName());

    // Keep references so we always shut everything down even if a test fails
    private final List<PeerServerImpl> servers = new ArrayList<>();
    private final Map<Long, InetSocketAddress> peerMap = new HashMap<>();

    private static final int BASE_UDP_PORT = 9800;
    private static final int CLUSTER_SIZE  = 5;        // odd size for a clean majority
    private static final long ELECTION_TIMEOUT_MS = 10_000; // hard ceiling for election
    private static final long POLL_INTERVAL_MS    = 50;

    @AfterEach
    void tearDown() {
        // Best-effort shutdown of all servers and joining threads
        for (PeerServerImpl s : servers) {
            try {
                s.shutdown();
            } catch (Exception ignore) {}
        }
        for (PeerServerImpl s : servers) {
            try {
                s.join(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
        servers.clear();
        peerMap.clear();
    }

    @Test
    @DisplayName("Elects exactly one leader (highest ID) and others follow")
    void testLeaderElectionHighestIdWins() {
        // Create IDs and ports
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L); // 5 is highest -> should be leader
        buildPeerMap(ids);

        // Start servers
        startCluster(ids);

        // Wait for convergence
        assertTimeoutPreemptively(Duration.ofMillis(ELECTION_TIMEOUT_MS), () -> {
            waitUntil(this::clusterConverged, ELECTION_TIMEOUT_MS, POLL_INTERVAL_MS);
        }, "Cluster did not converge to a single leader within the timeout");

        // Validate exactly one leader and the rest following
        List<PeerServerImpl> leaders = filterByState(ServerState.LEADING);
        List<PeerServerImpl> followers = filterByState(ServerState.FOLLOWING);

        assertEquals(1, leaders.size(), "There must be exactly one leader");
        assertEquals(CLUSTER_SIZE - 1, followers.size(), "All remaining peers must be FOLLOWING");

        // Validate the elected leader is the highest ID
        PeerServerImpl leader = leaders.get(0);
        long expectedLeaderId = ids.stream().max(Long::compareTo).orElseThrow();
        assertEquals(expectedLeaderId, leader.getServerId(), "Highest ID should be elected leader");

        // Validate that all peers agree on the current leader ID
        long agreedLeaderId = leader.getCurrentLeader().getProposedLeaderID();
        for (PeerServerImpl s : servers) {
            assertNotNull(s.getCurrentLeader(), "Each peer should have a non-null currentLeader");
            assertEquals(agreedLeaderId, s.getCurrentLeader().getProposedLeaderID(),
                    "All peers must agree on the same leader ID");
        }
    }

    @Test
    @DisplayName("Quorum size is majority (N/2 + 1)")
    void testQuorumSizeMajority() {
        List<Long> ids = Arrays.asList(10L, 11L, 12L, 13L, 14L);
        buildPeerMap(ids);
        startCluster(ids);

        // Just check the arithmetic on any one node
        assertFalse(servers.isEmpty());
        int expectedQuorum = (peerMap.size() / 2) + 1;
        assertEquals(expectedQuorum, servers.get(0).getQuorumSize());
    }

    @Test
    void demoStyleElectionEightPeers() throws Exception {
        Map<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

        // Create and start servers (each gets a copy of the map with itself removed)
        List<PeerServerImpl> servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> e : peerIDtoAddress.entrySet()) {
            Map<Long, InetSocketAddress> others = new HashMap<>(peerIDtoAddress);
            others.remove(e.getKey());
            PeerServerImpl s = new PeerServerImpl(
                    e.getValue().getPort(),
                    0L,
                     e.getKey(),
                     others
            );
            servers.add(s);
        }
        // Start all at roughly the same time
        for (PeerServerImpl s : servers) {
            s.start();
        }

        // Wait for convergence (poll instead of fixed sleep)
        long deadline = System.currentTimeMillis() + 10_000; // 10s max
        Long agreedLeader = null;
        while (System.currentTimeMillis() < deadline) {
            boolean allHaveLeaders = true;
            Long candidate = null;
            for (PeerServerImpl s : servers) {
                Vote v = s.getCurrentLeader();
                if (v == null || s.getPeerState() == PeerServer.ServerState.LOOKING) {
                    allHaveLeaders = false;
                    break;
                }
                if (candidate == null) candidate = v.getProposedLeaderID();
                if (!candidate.equals(v.getProposedLeaderID())) {
                    allHaveLeaders = false;
                    break;
                }
            }
            if (allHaveLeaders) {
                agreedLeader = candidate;
                break;
            }
            Thread.sleep(100);
        }

        try {
            // Assertions: must have converged
            assertNotNull(agreedLeader, "Cluster did not converge to a leader within the timeout");

            // Highest ID should win in the simplified algorithm (1..8 -> 8)
            long expectedLeader = Collections.max(peerIDtoAddress.keySet());
            assertEquals(expectedLeader, agreedLeader.longValue(), "Highest ID should be elected leader");

            // Exactly one LEADING, rest FOLLOWING, and everyone agrees on leader ID
            int leaders = 0;
            for (PeerServerImpl s : servers) {
                if (s.getPeerState() == PeerServer.ServerState.LEADING) {
                    leaders++;
                    assertEquals(expectedLeader, s.getServerId(), "Only the highest-ID server should be LEADING");
                } else {
                    assertEquals(PeerServer.ServerState.FOLLOWING, s.getPeerState(), "Non-leaders must be FOLLOWING");
                }
                assertNotNull(s.getCurrentLeader());
                assertEquals(expectedLeader, s.getCurrentLeader().getProposedLeaderID(), "All peers must agree on leader ID");
                // (Optional) print like the demo:
                System.out.println("Server on port " + s.getAddress().getPort()
                        + " whose ID is " + s.getServerId()
                        + " has leader " + s.getCurrentLeader().getProposedLeaderID()
                        + " and state " + s.getPeerState().name());
            }
            assertEquals(1, leaders, "There must be exactly one leader");
        } finally {
            // Clean shutdown and join
            for (PeerServerImpl s : servers) {
                try { s.shutdown(); } catch (Exception ignore) {}
            }
            for (PeerServerImpl s : servers) {
                try { s.join(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }


    // ----------------------
    // Helpers
    // ----------------------

    private void buildPeerMap(List<Long> ids) {
        peerMap.clear();
        for (int i = 0; i < ids.size(); i++) {
            long id = ids.get(i);
            int port = BASE_UDP_PORT + i;
            peerMap.put(id, new InetSocketAddress("localhost", port));
        }
        assertEquals(CLUSTER_SIZE, peerMap.size(), "Peer map must match cluster size");
    }

    private void startCluster(List<Long> ids) {
        servers.clear();
        // Same peer map passed to every server
        for (Long id : ids) {
            int port = peerMap.get(id).getPort();
            PeerServerImpl peer = new PeerServerImpl(
                    port,                  // my UDP port
                    /* peerEpoch */ 0L,    // start at 0; election code will manage per-round semantics
                    id,                    // my ID
                    peerMap                // all peers
            );
            servers.add(peer);
        }
        // Start all at (roughly) the same time to trigger election
        for (PeerServerImpl s : servers) {
            s.start();
        }
    }

    private boolean clusterConverged() {
        List<PeerServerImpl> leaders = filterByState(ServerState.LEADING);
        List<PeerServerImpl> followers = filterByState(ServerState.FOLLOWING);

        // Must be exactly one leader, and all others following
        if (leaders.size() != 1) return false;
        if (leaders.size() + followers.size() != CLUSTER_SIZE) return false;

        // Everyone must agree on the same leader ID
        long leaderId = leaders.get(0).getServerId();
        for (PeerServerImpl s : servers) {
            if (s.getCurrentLeader() == null) return false;
            if (s.getCurrentLeader().getProposedLeaderID() != leaderId) return false;
        }
        return true;
    }

    private List<PeerServerImpl> filterByState(ServerState state) {
        List<PeerServerImpl> result = new ArrayList<>();
        for (PeerServerImpl s : servers) {
            if (s.getPeerState() == state) {
                result.add(s);
            }
        }
        return result;
    }

    private void waitUntil(SupplierWithException<Boolean> condition,
                           long timeoutMs,
                           long pollIntervalMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) return;
            Thread.sleep(pollIntervalMs);
        }
        fail("Condition not met within timeout");
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get() throws Exception;
    }
}