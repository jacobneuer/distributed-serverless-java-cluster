package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class Stage3Test {

    private final int[] ports = {8010, 8020, 8030, 8040};

    private InetSocketAddress clientAddr;
    private LinkedBlockingQueue<Message> outgoing;  // client → leader
    private LinkedBlockingQueue<Message> incoming;  // leader → client

    private ArrayList<PeerServerImpl> servers;

    @BeforeEach
    public void setup() throws Exception {

        int clientPort = 9000 + new Random().nextInt(5000);
        this.clientAddr = new InetSocketAddress("localhost", clientPort);

        this.outgoing = new LinkedBlockingQueue<>();
        this.incoming = new LinkedBlockingQueue<>();

        // Start servers FIRST
        createServers();
        Thread.sleep(5000);

        // Now start client networking
        UDPMessageSender sender = new UDPMessageSender(outgoing, clientPort);
        Util.startAsDaemon(sender, "client-sender");

        UDPMessageReceiver receiver = new UDPMessageReceiver(incoming, clientAddr, clientPort, null);
        Util.startAsDaemon(receiver, "client-receiver");

        Thread.sleep(300); // let sockets bind
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        for (PeerServerImpl server : servers) {
            try {
                server.shutdown();
            } catch (Exception ignored) {}
        }

        // Give OS time to release UDP ports
        Thread.sleep(3000);
    }

    private void createServers() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();

        for (int i = 0; i < ports.length; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", ports[i]));
        }

        servers = new ArrayList<>();

        for (Map.Entry<Long, InetSocketAddress> entry : peerMap.entrySet()) {
            HashMap<Long, InetSocketAddress> mapCopy = new HashMap<>(peerMap);
            mapCopy.remove(entry.getKey());  // remove itself

            PeerServerImpl server = new PeerServerImpl(
                    entry.getValue().getPort(),   // myPort
                    0,                            // peerEpoch
                    entry.getKey(),               // id
                    mapCopy                       // others
            );

            servers.add(server);
            server.start();
        }
    }

    @Test
    public void testLeaderElection() {
        boolean foundLeader = servers.stream()
                .anyMatch(s -> s.getPeerState() == PeerServer.ServerState.LEADING);

        assertTrue(foundLeader, "No server reached LEADING state!");
    }

    @Test
    public void testWorkDistributionAndExecution() throws Exception {

        PeerServerImpl leader = servers.stream()
                .filter(s -> s.getPeerState() == PeerServer.ServerState.LEADING)
                .findFirst()
                .orElse(null);

        assertNotNull(leader, "Leader was not elected!");

        int leaderPort = leader.getUdpPort();

        // Send WORK messages to leader
        for (int i = 0; i < ports.length; i++) {
            String code = generateClass("Hello request " + i);

            Message workMsg = new Message(
                    Message.MessageType.WORK,
                    code.getBytes(),
                    clientAddr.getHostString(),
                    clientAddr.getPort(),
                    "localhost",
                    leaderPort,
                    i
            );

            outgoing.put(workMsg);
        }

        // Expect N responses (asynchronous)
        for (int i = 0; i < ports.length; i++) {
            Message response = incoming.take();

            assertEquals(Message.MessageType.COMPLETED_WORK, response.getMessageType(),
                    "Leader did not forward COMPLETED_WORK properly.");
            // Log
            System.out.println("Received response from worker " + i + ": " + new String(response.getMessageContents()));

            String output = new String(response.getMessageContents());

            assertTrue(output.contains("Hello request"),
                    "Worker output incorrect: " + output);
            // Log
            System.out.println("Worker " + i + " output: " + output);
        }
    }

    @Test
    public void testRoundRobinWorkerDistribution() throws Exception {
        PeerServerImpl leader = servers.stream()
                .filter(s -> s.getPeerState() == PeerServer.ServerState.LEADING)
                .findFirst()
                .orElse(null);

        assertNotNull(leader);

        int leaderPort = leader.getUdpPort();
        int N = 12; // send more tasks than workers

        for (int i = 0; i < N; i++) {
            String code = generateClass("RR " + i);
            Message msg = new Message(
                    Message.MessageType.WORK,
                    code.getBytes(),
                    clientAddr.getHostString(),
                    clientAddr.getPort(),
                    "localhost",
                    leaderPort
            );
            outgoing.put(msg);
        }

        // collect outputs
        Set<String> outputs = new HashSet<>();
        for (int i = 0; i < N; i++) {
            Message reply = incoming.take();
            outputs.add(new String(reply.getMessageContents()));
        }

        assertEquals(N, outputs.size());
    }

    @Test
    public void testCompilationFailureReturnsError() throws Exception {
        PeerServerImpl leader = servers.stream()
                .filter(s -> s.getPeerState() == PeerServer.ServerState.LEADING)
                .findFirst()
                .orElse(null);

        assertNotNull(leader);

        String badCode = "public class X { public String run() { return 123; } }"; // invalid

        Message msg = new Message(
                Message.MessageType.WORK,
                badCode.getBytes(),
                clientAddr.getHostString(),
                clientAddr.getPort(),
                "localhost",
                leader.getUdpPort()
        );

        outgoing.put(msg);

        Message reply = incoming.take();
        String out = new String(reply.getMessageContents());

        assertTrue(out.contains("Error") || out.contains("did not compile") || out.contains("line"));
    }

    @Test
    public void testRequestIDUniqueness() throws Exception {
        PeerServerImpl leader = servers.stream()
                .filter(s -> s.getPeerState() == PeerServer.ServerState.LEADING)
                .findFirst()
                .orElse(null);
        assertNotNull(leader);
        int leaderPort = leader.getUdpPort();

        int N = 8;
        for (int i = 0; i < N; i++) {
            outgoing.put(new Message(
                    Message.MessageType.WORK,
                    generateClass("ID " + i).getBytes(),
                    clientAddr.getHostString(), clientAddr.getPort(),
                    "localhost", leaderPort
            ));
        }

        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < N; i++) {
            Message reply = incoming.take();
            ids.add(reply.getRequestID());
        }

        assertEquals(N, ids.size(), "Duplicate request IDs were generated!");
    }

    @Test
    public void stressTestManyWorkers() throws Exception {
        // --- 1. Wait for leader to emerge ---
        PeerServerImpl leader = servers.stream()
                .filter(s -> s.getPeerState() == PeerServer.ServerState.LEADING)
                .findFirst()
                .orElse(null);
        assertNotNull(leader);

        assertNotNull(leader, "Leader was not elected in time!");
        int leaderPort = leader.getUdpPort();

        final int NUM_REQUESTS = 200;   // stress load
        final int TIMEOUT_MS = 10000;    // max time to wait for all replies

        // --- 2. Send many WORK messages as fast as possible ---
        for (int i = 0; i < NUM_REQUESTS; i++) {
            String classSrc = generateClass("Stress_" + i);

            Message workMsg = new Message(
                    Message.MessageType.WORK,
                    classSrc.getBytes(),
                    clientAddr.getHostString(),
                    clientAddr.getPort(),
                    "localhost",
                    leaderPort
            );

            outgoing.put(workMsg); // no delay
        }

        // --- 3. Collect replies ---
        long start = System.currentTimeMillis();
        Set<Long> requestIDs = new HashSet<>();
        int received = 0;

        while (received < NUM_REQUESTS &&
                System.currentTimeMillis() - start < TIMEOUT_MS) {

            Message reply = incoming.poll(200, TimeUnit.MILLISECONDS);
            if (reply == null) continue;

            assertEquals(Message.MessageType.COMPLETED_WORK, reply.getMessageType(),
                    "Received non-completed message during stress test!");

            String out = new String(reply.getMessageContents());
            assertTrue(out.contains("Stress_"), "Bad worker output: " + out);

            requestIDs.add(reply.getRequestID());
            received++;
        }

        // --- 4. Validate results ---
        assertEquals(NUM_REQUESTS, received,
                "Did not receive all worker results in time!");

        assertEquals(NUM_REQUESTS, requestIDs.size(),
                "Duplicate or missing request IDs under load!");
    }


    private String generateClass(String msg) {
        return "public class HW { public String run(){ return \"" + msg + "\"; }}";
    }
}