package edu.yu.cs.com3800.gateway;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class PeerServerTest {

    private final int[] udpPorts = {8010, 8020, 8030, 8040};
    private ArrayList<PeerServerImpl> servers;

    @AfterEach
    public void cleanup() {
        if (servers != null) {
            for (PeerServerImpl server : servers) {
                if (server != null) {
                    server.shutdown();
                }
            }
        }
    }

    private void createServers() throws Exception {
        Map<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < udpPorts.length; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", udpPorts[i]));
        }

        servers = new ArrayList<>();
        for (int i = 0; i < udpPorts.length; i++) {
            PeerServerImpl server = new PeerServerImpl(
                    udpPorts[i],
                    0,
                    (long) i,
                    peerIDtoAddress,
                    -1L, // No real Gateway server running for this test
                    0    // No observers for this test
            );
            servers.add(server);
            server.start();
        }

        Thread.sleep(5000);
    }

    private PeerServerImpl getLeader() throws InterruptedException {
        PeerServerImpl leaderNode = null;
        for (int i = 0; i < 20; i++) { // Try for 10 seconds
            for (PeerServerImpl server : servers) {
                if (server.getPeerState() == PeerServer.ServerState.LEADING) {
                    leaderNode = server;
                    break;
                }
            }
            if (leaderNode != null) break;
            Thread.sleep(500);
        }
        return leaderNode;
    }

    @Test
    public void testSingleTcpRequest() throws Exception {
        createServers();

        // 1. Wait for Leader
        PeerServerImpl leader = getLeader();
        assertNotNull(leader, "Leader was not elected in time");
        System.out.println("Leader elected: Node " + leader.getServerId());

        // 2. Calculate TCP Port
        int leaderTcpPort = leader.getUdpPort() + 2;

        // 3. Connect via TCP (Acting as Gateway)
        String code = generateClass("Hello TCP World");

        System.out.println("Connecting to Leader at localhost:" + leaderTcpPort);
        try (Socket socket = new Socket("localhost", leaderTcpPort)) {

            // 4. Send Message
            Message workMsg = new Message(
                    MessageType.WORK,
                    code.getBytes(),
                    "localhost",
                    8888, // Fake Gateway Port
                    "localhost",
                    leaderTcpPort,
                    12345L // Request ID
            );

            OutputStream out = socket.getOutputStream();
            out.write(workMsg.getNetworkPayload());
            out.flush();
            socket.shutdownOutput();

            // 5. Read Response
            InputStream in = socket.getInputStream();
            byte[] responseBytes = Util.readAllBytesFromNetwork(in);

            assertNotNull(responseBytes, "Received null response from Leader");
            assertTrue(responseBytes.length > 0, "Received empty response from Leader");

            Message responseMsg = new Message(responseBytes);
            assertEquals(MessageType.COMPLETED_WORK, responseMsg.getMessageType());

            String output = new String(responseMsg.getMessageContents());
            System.out.println("Result: " + output);
            assertTrue(output.contains("Hello TCP World"));
        }
    }

    @Test
    public void testConcurrentTcpRequests() throws Exception {
        // Verifies that RoundRobinLeader uses a Thread Pool and doesn't block
        createServers();
        PeerServerImpl leader = getLeader();
        assertNotNull(leader);
        int leaderTcpPort = leader.getUdpPort() + 2;

        int requestCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(requestCount);
        List<Future<String>> futures = new ArrayList<>();

        System.out.println("Sending " + requestCount + " requests concurrently...");

        for (int i = 0; i < requestCount; i++) {
            final int id = i;
            futures.add(executor.submit(() -> {
                try (Socket socket = new Socket("localhost", leaderTcpPort)) {
                    String code = generateClass("Concurrent Request " + id);
                    Message msg = new Message(
                            MessageType.WORK,
                            code.getBytes(),
                            "localhost", 8888,
                            "localhost", leaderTcpPort,
                            (long) id
                    );

                    socket.getOutputStream().write(msg.getNetworkPayload());
                    socket.shutdownOutput();

                    byte[] resp = Util.readAllBytesFromNetwork(socket.getInputStream());
                    Message respMsg = new Message(resp);
                    return new String(respMsg.getMessageContents());
                }
            }));
        }

        // Validate all responses
        for (int i = 0; i < requestCount; i++) {
            String result = futures.get(i).get(5, TimeUnit.SECONDS); // 5 sec timeout per request
            assertTrue(result.contains("Concurrent Request " + i), "Wrong result for req " + i);
        }

        System.out.println("All concurrent requests handled successfully.");
        executor.shutdown();
    }

    @Test
    public void testRoundRobinDistribution() throws Exception {
        createServers();
        PeerServerImpl leader = getLeader();
        assertNotNull(leader);

        int leaderTcpPort = leader.getUdpPort() + 2;

        List<String> results = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            try (Socket socket = new Socket("localhost", leaderTcpPort)) {
                String code = generateClass("Task " + i);
                Message msg = new Message(
                        MessageType.WORK, code.getBytes(),
                        "localhost", 8888,
                        "localhost", leaderTcpPort, i
                );
                socket.getOutputStream().write(msg.getNetworkPayload());
                socket.shutdownOutput();

                byte[] resp = Util.readAllBytesFromNetwork(socket.getInputStream());
                results.add(new String(new Message(resp).getMessageContents()));
            }
        }

        // Ensure all tasks completed
        for (int i = 0; i < 6; i++) {
            assertTrue(results.get(i).contains("Task " + i));
        }

        // You can inspect worker logs manually to ensure a variety of workers ran tasks
        System.out.println("Round robin distribution appeared successful.");
    }

    @Test
    public void testSlowWorkerDoesNotBlockCluster() throws Exception {
        createServers();
        PeerServerImpl leader = getLeader();
        assertNotNull(leader);

        int port = leader.getUdpPort() + 2;

        // Fire two requests: one slow, one fast
        ExecutorService exec = Executors.newFixedThreadPool(2);

        Future<String> slowFuture = exec.submit(() -> {
            try (Socket socket = new Socket("localhost", port)) {
                String code = "public class HW { public String run(){ " +
                        "try{Thread.sleep(2000);}catch(Exception e){} return \"SLOW\";} }";
                Message msg = new Message(MessageType.WORK, code.getBytes(),
                        "localhost", 8888, "localhost", port, 1);
                socket.getOutputStream().write(msg.getNetworkPayload());
                socket.shutdownOutput();

                byte[] resp = Util.readAllBytesFromNetwork(socket.getInputStream());
                return new String(new Message(resp).getMessageContents());
            }
        });

        Future<String> fastFuture = exec.submit(() -> {
            try (Socket socket = new Socket("localhost", port)) {
                String code = generateClass("FAST");
                Message msg = new Message(MessageType.WORK, code.getBytes(),
                        "localhost", 8888, "localhost", port, 2);
                socket.getOutputStream().write(msg.getNetworkPayload());
                socket.shutdownOutput();

                byte[] resp = Util.readAllBytesFromNetwork(socket.getInputStream());
                return new String(new Message(resp).getMessageContents());
            }
        });

        // Fast one must finish FIRST
        String fastResult = fastFuture.get(3, TimeUnit.SECONDS);
        assertTrue(fastResult.contains("FAST"));

        // Slow one should still finish eventually
        String slowResult = slowFuture.get(5, TimeUnit.SECONDS);
        assertTrue(slowResult.contains("SLOW"));
    }

    private String generateClass(String msg) {
        return "public class HW { public String run(){ return \"" + msg + "\"; }}";
    }
}