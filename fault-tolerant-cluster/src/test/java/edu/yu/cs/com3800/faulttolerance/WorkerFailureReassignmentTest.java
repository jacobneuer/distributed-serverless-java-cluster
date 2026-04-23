package edu.yu.cs.com3800.faulttolerance;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerFailureReassignmentTest {

    private static final int HTTP_PORT = 9100;
    private static final int GATEWAY_UDP = 9000;

    private GatewayServer gateway;
    private PeerServerImpl leader;
    private PeerServerImpl worker1;
    private PeerServerImpl worker2;

    private List<PeerServerImpl> servers;

    @BeforeEach
    void setup() throws Exception {
        servers = new ArrayList<>();

        ConcurrentHashMap<Long, InetSocketAddress> peerMap = new ConcurrentHashMap<>();

        // IDs:
        // 0 = gateway (observer)
        // 1 = leader
        // 2,3 = workers
        peerMap.put(0L, new InetSocketAddress("localhost", GATEWAY_UDP));
        peerMap.put(1L, new InetSocketAddress("localhost", 9010));
        peerMap.put(2L, new InetSocketAddress("localhost", 9020));
        peerMap.put(3L, new InetSocketAddress("localhost", 9030));

        // Start workers
        worker1 = new PeerServerImpl(9020, 0, 2L, peerMap, 0L, 1);
        worker2 = new PeerServerImpl(9030, 0, 3L, peerMap, 0L, 1);

        // Start leader
        leader = new PeerServerImpl(9010, 0, 1L, peerMap, 0L, 1);

        servers.add(worker1);
        servers.add(worker2);
        servers.add(leader);

        for (PeerServerImpl s : servers) {
            s.start();
        }

        // Start gateway (observer)
        gateway = new GatewayServer(
                HTTP_PORT,
                GATEWAY_UDP,
                0,
                0L,
                peerMap,
                1
        );
        gateway.start();

        // Allow gossip + election to stabilize
        Thread.sleep(6000);
    }

    @AfterEach
    void teardown() {
        for (PeerServerImpl s : servers) {
            try { s.shutdown(); } catch (Exception ignored) {}
        }
        try { gateway.shutdown(); } catch (Exception ignored) {}
    }

    @Test
    void testWorkerFailureDoesNotLoseRequest() throws Exception {
        String code =
                "public class HW { " +
                        "  public String run() { " +
                        "    try { Thread.sleep(1500); } catch(Exception e) {} " +
                        "    return \"Recovered\"; " +
                        "  } " +
                        "}";

        // Fire HTTP request asynchronously
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<String> response = pool.submit(() -> {
            HttpURLConnection conn = post(code);
            assertEquals(200, conn.getResponseCode());
            return read(conn.getInputStream()).trim();
        });

        // Give leader time to dispatch work
        Thread.sleep(300);

        // Kill worker mid-flight
        worker1.shutdown();

        // Wait until gateway observes worker failure
        waitUntil(() ->
                        gateway.getPeerServer().isFailed(2L),
                45_000
        );

        // Client must still receive a response
        assertEquals("Recovered", response.get(30, TimeUnit.SECONDS));

        pool.shutdown();
    }

    private HttpURLConnection post(String body) throws Exception {
        URL url = new URL("http://localhost:" + HTTP_PORT + "/compileandrun");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "text/x-java-source");
        conn.setRequestProperty("Accept", "text/plain");

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return conn;
    }

    private String read(InputStream in) throws Exception {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    private void waitUntil(Callable<Boolean> condition, long timeoutMs) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (condition.call()) return;
            Thread.sleep(100);
        }
        fail("Condition not met within timeout");
    }
}