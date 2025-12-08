package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ElectionObserverTest {

    private PeerServerImpl s1, s2, observer;
    private Map<Long, InetSocketAddress> peerMap;

    @BeforeEach
    void setup() throws InterruptedException {
        // Wait a couple seconds for the sockets to reset
        Thread.sleep(5000);

        peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 9001));
        peerMap.put(2L, new InetSocketAddress("localhost", 9002));
        peerMap.put(3L, new InetSocketAddress("localhost", 9003)); // observer
    }

    @AfterEach
    void teardown() {
        if (s1 != null) s1.shutdown();
        if (s2 != null) s2.shutdown();
        if (observer != null) observer.shutdown();
    }

    private void startCluster() throws Exception {
        s1 = new PeerServerImpl(9001, 0, 1L, peerMap, 3L, 1);
        s2 = new PeerServerImpl(9002, 0, 2L, peerMap, 3L, 1);
        observer = new PeerServerImpl(9003, 0, 3L, peerMap, 3L, 1);

        s1.setPeerState(PeerServer.ServerState.LOOKING);
        s2.setPeerState(PeerServer.ServerState.LOOKING);
        observer.setPeerState(PeerServer.ServerState.OBSERVER);

        s1.start();
        s2.start();
        observer.start();
    }

    // --------------------------------------------------------------------
    // TEST 1: Basic Election With Observer
    // --------------------------------------------------------------------
    @Test
    void testObserverLearnsLeader() throws Exception {
        startCluster();
        Thread.sleep(4000);

        Vote v1 = s1.getCurrentLeader();
        Vote v2 = s2.getCurrentLeader();
        Vote vo = observer.getCurrentLeader();

        assertNotNull(v1, "s1 should have a leader");
        assertNotNull(v2, "s2 should have a leader");
        assertNotNull(vo, "observer should learn the leader");

        assertEquals(v1.getProposedLeaderID(), v2.getProposedLeaderID(),
                "both voters must agree on leader");

        assertEquals(v1.getProposedLeaderID(), vo.getProposedLeaderID(),
                "observer must match actual elected leader");
    }

    // --------------------------------------------------------------------
    // TEST 2: Observer does not vote or affect quorum
    // --------------------------------------------------------------------
    @Test
    void testObserverDoesNotCountTowardsQuorum() throws Exception {
        startCluster();
        Thread.sleep(4000);

        // quorum size = (votersOnlyCount / 2) + 1 = ((2) / 2) + 1 = 2
        assertEquals(2, s1.getQuorumSize(), "quorum should ignore observers");
        assertEquals(2, s2.getQuorumSize());
    }

    // --------------------------------------------------------------------
    // TEST 3: Leader must be highest ID (2 is > 1)
    // --------------------------------------------------------------------
    @Test
    void testLeaderIsHighestID() throws Exception {
        startCluster();
        Thread.sleep(4000);

        assertEquals(2L, s1.getCurrentLeader().getProposedLeaderID(),
                "highest ID must win election");
    }

    // --------------------------------------------------------------------
    // TEST 4: Observer starts late but must still learn leader
    // --------------------------------------------------------------------
    @Test
    @Disabled
    void testObserverJoinsLate() throws Exception {

        // start only the voters
        s1 = new PeerServerImpl(9001, 0, 1L, peerMap, 3L, 1);
        s2 = new PeerServerImpl(9002, 0, 2L, peerMap, 3L, 1);

        s1.setPeerState(PeerServer.ServerState.LOOKING);
        s2.setPeerState(PeerServer.ServerState.LOOKING);

        s1.start();
        s2.start();

        Thread.sleep(4000); // voters elect a leader

        // NOW start observer late
        observer = new PeerServerImpl(9003, 0, 3L, peerMap, 3L, 1);
        observer.setPeerState(PeerServer.ServerState.OBSERVER);
        observer.start();

        Thread.sleep(4000);

        assertNotNull(observer.getCurrentLeader(),
                "late observer must still learn the leader");
    }

    // --------------------------------------------------------------------
    // TEST 5: Voter is slow but election must still converge
    // --------------------------------------------------------------------
    @Test
    void testSlowVoter() throws Exception {

        // intentionally do NOT start s2 immediately
        s1 = new PeerServerImpl(9001, 0, 1L, peerMap, 3L, 1);
        s2 = new PeerServerImpl(9002, 0, 2L, peerMap, 3L, 1);
        observer = new PeerServerImpl(9003, 0, 3L, peerMap, 3L, 1);

        s1.setPeerState(PeerServer.ServerState.LOOKING);
        observer.setPeerState(PeerServer.ServerState.OBSERVER);

        s1.start();
        observer.start();

        Thread.sleep(3000);

        s2.setPeerState(PeerServer.ServerState.LOOKING);
        s2.start();

        Thread.sleep(8000);

        assertNotNull(s1.getCurrentLeader());
        assertNotNull(s2.getCurrentLeader());
        assertNotNull(observer.getCurrentLeader());

        assertEquals(s1.getCurrentLeader().getProposedLeaderID(),
                s2.getCurrentLeader().getProposedLeaderID());
    }

    @Test
    void testElectionWithTenPeers() throws Exception {
        // ---- 1. Build the peer map ----
        Map<Long, InetSocketAddress> map = new HashMap<>();
        int basePort = 10000;
        for (long i = 1; i <= 10; i++) {
            map.put(i, new InetSocketAddress("localhost", basePort + (int)i));
        }

        // ---- 2. Create 10 servers ----
        PeerServerImpl[] servers = new PeerServerImpl[10];

        for (int i = 0; i < 10; i++) {
            long id = i + 1;
            servers[i] = new PeerServerImpl(
                    basePort + (int)id,
                    0,
                    id,
                    map,
                    10L,        // observer is node 10
                    1           // 1 observer
            );

            if (id == 10) {
                servers[i].setPeerState(PeerServer.ServerState.OBSERVER);
            } else {
                servers[i].setPeerState(PeerServer.ServerState.LOOKING);
            }
        }

        // ---- 3. Start all servers ----
        for (PeerServerImpl s : servers) {
            s.start();
        }

        // ---- 4. Allow election to complete ----
        Thread.sleep(6000);

        // ---- 5. Check leader consistency ----
        Vote leader = servers[0].getCurrentLeader();
        assertNotNull(leader, "Cluster should elect a leader");

        long leaderId = leader.getProposedLeaderID();
        long leaderEpoch = leader.getPeerEpoch();

        System.out.printf("Leader elected: %d (epoch %d)%n", leaderId, leaderEpoch);

        // All voters should have the same leader
        for (int i = 0; i < 9; i++) {
            Vote v = servers[i].getCurrentLeader();
            assertNotNull(v, "Voter " + (i+1) + " must know the leader");
            assertEquals(leaderId, v.getProposedLeaderID(),
                    "Voter " + (i+1) + " does not agree on the leader");
        }

        // Observer should also know the leader
        Vote observerVote = servers[9].getCurrentLeader();
        assertNotNull(observerVote, "Observer must learn the leader");
        assertEquals(leaderId, observerVote.getProposedLeaderID(),
                "Observer does not agree on the leader");

        // ---- 6. Shutdown servers ----
        for (PeerServerImpl s : servers) {
            s.shutdown();
        }
    }
}

