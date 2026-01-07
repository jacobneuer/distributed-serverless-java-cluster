package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class NewGatewayServerTest {

    private GatewayServer gateway;
    private final int httpPort = 8888;

    private PeerServerImpl s1, s2, s3;
    private ConcurrentHashMap<Long, InetSocketAddress> peerMap;

    @BeforeEach
    void setupCluster() throws Exception {

        peerMap = new ConcurrentHashMap<>();

        peerMap.put(0L, new InetSocketAddress("localhost", 8040));
        peerMap.put(1L, new InetSocketAddress("localhost", 8010));
        peerMap.put(2L, new InetSocketAddress("localhost", 8020));
        peerMap.put(3L, new InetSocketAddress("localhost", 8030));

        s1 = new PeerServerImpl(8010, 0, 1L, peerMap, 0L, 1);
        s2 = new PeerServerImpl(8020, 0, 2L, peerMap, 0L, 1);
        s3 = new PeerServerImpl(8030, 0, 3L, peerMap, 0L, 1);

        s1.start();
        s2.start();
        s3.start();

        gateway = new GatewayServer(
                httpPort,
                8040,
                0,
                0L,
                peerMap,
                1
        );

        gateway.start();

        // Allow election + observer stabilization
        Thread.sleep(6000);
    }

    @AfterEach
    void teardown() {
        try { s1.shutdown(); } catch (Exception ignored) {}
        try { s2.shutdown(); } catch (Exception ignored) {}
        try { s3.shutdown(); } catch (Exception ignored) {}
        try { gateway.shutdown(); } catch (Exception ignored) {}
    }

    @Test
    @Order(1)
    void testBasicCompileAndRun() throws Exception {
        String body = "public class HW { public String run(){ return \"Hello\"; }}";

        HttpURLConnection conn = post(body);

        assertEquals(200, conn.getResponseCode());
        assertEquals("false", conn.getHeaderField("Cached-Response"));

        String result = read(conn.getInputStream());
        assertEquals("Hello", result.trim());
    }

    @Test
    @Order(2)
    void testCompileError() throws Exception {
        String badCode = "public class HW { public String run(){ BAD CODE }}";

        HttpURLConnection conn = post(badCode);

        assertEquals(400, conn.getResponseCode());
        assertEquals("false", conn.getHeaderField("Cached-Response"));

        String result = read(conn.getErrorStream());
        assertTrue(result.contains("Exception"));
    }

    @Test
    @Order(3)
    void testCaching() throws Exception {
        String code = "public class HW { public String run(){ return \"CacheTest\"; }}";

        HttpURLConnection conn1 = post(code);
        assertEquals(200, conn1.getResponseCode());
        assertEquals("false", conn1.getHeaderField("Cached-Response"));
        assertEquals("CacheTest", read(conn1.getInputStream()).trim());

        HttpURLConnection conn2 = post(code);
        assertEquals(200, conn2.getResponseCode());
        assertEquals("true", conn2.getHeaderField("Cached-Response"));
        assertEquals("CacheTest", read(conn2.getInputStream()).trim());
    }

    @Test
    @Order(4)
    void testRejectNonPostRequest() throws Exception {
        URL url = new URL("http://localhost:" + httpPort + "/compileandrun");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("GET");
        conn.connect();

        assertEquals(405, conn.getResponseCode());
    }

    @Test
    @Order(5)
    void testRejectWrongContentType() throws Exception {
        URL url = new URL("http://localhost:" + httpPort + "/compileandrun");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "text/plain");

        try (OutputStream os = conn.getOutputStream()) {
            os.write("hello".getBytes(StandardCharsets.UTF_8));
        }

        assertEquals(400, conn.getResponseCode());
    }

    @Test
    @Order(6)
    void testGatewayRemainsObserver() {
        assertEquals(PeerServer.ServerState.OBSERVER,
                gateway.getPeerServer().getPeerState());
    }

    @Test
    @Order(7)
    void testConcurrentRequests() throws Exception {
        int N = 10;
        ExecutorService pool = Executors.newFixedThreadPool(N);
        List<Future<String>> futures = new ArrayList<>();

        String code = "public class HW { public String run(){ return \"X\"; }}";

        for (int i = 0; i < N; i++) {
            futures.add(pool.submit(() -> {
                HttpURLConnection conn = post(code);
                assertEquals(200, conn.getResponseCode());
                return read(conn.getInputStream()).trim();
            }));
        }

        for (Future<String> f : futures) {
            assertEquals("X", f.get(5, TimeUnit.SECONDS));
        }

        pool.shutdown();
    }

    private HttpURLConnection post(String body) throws Exception {
        URL url = new URL("http://localhost:" + httpPort + "/compileandrun");
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
}