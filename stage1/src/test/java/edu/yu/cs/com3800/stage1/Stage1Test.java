package edu.yu.cs.com3800.stage1;

import edu.yu.cs.stage1.Client;
import edu.yu.cs.stage1.ClientImpl;
import edu.yu.cs.stage1.SimpleServerImpl;
import jdk.jfr.Name;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;


public class Stage1Test {

    private static SimpleServerImpl server;
    private static final int port = 9000; // default port
    private static final HttpClient rawClient = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() throws Exception {
        server = new SimpleServerImpl(9000);
        server.start();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    private static URI endpoint() {
        return URI.create("http://localhost:" + port + "/compileandrun");
    }

    @Test
    void clientSuccessReturns200AndRunBody() throws Exception {
        Client client = new ClientImpl("localhost", port);

        String javaSource = """
                public class Hello {
                    public Hello() {}
                    public String run() { return "Hello, world!"; }
                }
                """;

        client.sendCompileAndRunRequest(javaSource);
        Client.Response response = client.getResponse();

        int expectedCode = 200;
        String expectedBody = "Hello, world!";
        int actualCode = response.getCode();
        String actualBody = response.getBody();

        // Print expected and actual responses
        System.out.println("Expected response:");
        System.out.println("Status code: " + expectedCode);
        System.out.println("Body: " + expectedBody);
        System.out.println("Actual response:");
        System.out.println("Status code: " + actualCode);
        System.out.println("Body: " + actualBody);

        assertEquals(200, response.getCode());
        assertEquals("Hello, world!", response.getBody());
    }

    // ---------- Failure path (JavaRunner throws) ----------

    @Test
    void clientFailureReturns400WithMessageAndStacktrace() throws Exception {
        Client client = new ClientImpl("localhost", port);

        // Wrong run() return type should trigger JavaRunner to throw
        String badSource = """
                public class Bad {
                    public Bad() {}
                    public void run() {}
                }
                """;

        client.sendCompileAndRunRequest(badSource);
        Client.Response response = client.getResponse();

        int expectedCode = 400;

        // Print expected and actual responses
        System.out.println("Expected response:");
        System.out.println("Status code: " + expectedCode);
        System.out.println("Actual response:");
        System.out.println("Status code: " + response.getCode());

        // Verify response body
        assertEquals(expectedCode, response.getCode(), "Status code mismatch");
        assertNotNull(response.getBody());

        // Must contain message + newline + stacktrace (handler formats this)
        assertTrue(response.getBody().contains("\n"), "Body should contain newline separating message and stack trace");
        assertTrue(response.getBody().contains("\n\tat ") || response.getBody().contains("\nat "),
                "Body should include stack trace lines (e.g., '\\tat ...')");
    }

    @Test
    void nonPostRequestReturns405AndAllowHeader() throws Exception {
        // Use raw HttpClient to send GET request
        HttpClient rawClient = HttpClient.newHttpClient();

        HttpRequest req = HttpRequest.newBuilder(endpoint())
                .GET()
                .build();

        HttpResponse<String> response = rawClient.send(req, HttpResponse.BodyHandlers.ofString());

        int expectedCode = 405;

        // Print expected and actual responses
        System.out.println("Expected response:");
        System.out.println("Status code: " + expectedCode);
        System.out.println("Actual response:");
        System.out.println("Status code: " + response.statusCode());

        assertEquals(405, response.statusCode());
        assertEquals("POST", response.headers().firstValue("Allow").orElse(""),
                "Server must advertise POST as allowed");
    }

    @Test
    void wrongContentTypeReturns400() throws Exception {
        String okSource = """
                public class X {
                    public String run() { return "ok"; }
                }
                """;

        HttpRequest req = HttpRequest.newBuilder(endpoint())
                .POST(HttpRequest.BodyPublishers.ofString(okSource))
                .header("Content-Type", "text/plain")
                .build();

        HttpResponse<String> response = rawClient.send(req, HttpResponse.BodyHandlers.ofString());

        int expectedCode = 400;

        // Print expected and actual responses
        System.out.println("Expected response:");
        System.out.println("Status code: " + expectedCode);
        System.out.println("Actual response:");
        System.out.println("Status code: " + response.statusCode());

        assertEquals(400, response.statusCode(), "Server must reject non text/x-java-source content type");
    }

    // ---------- Client behavior edge case ----------

    @Test
    void getResponseWithoutSendThrowsIOException() {
        Client client;
        try {
            client = new ClientImpl("localhost", port);
        } catch (IOException e) {
            fail("Client construction failed unexpectedly: " + e.getMessage());
            return;
        }

        IOException ex = assertThrows(IOException.class, client::getResponse,
                "Calling getResponse() before send should throw IOException");
        assertTrue(ex.getMessage() != null && ex.getMessage().toLowerCase().contains("no response"),
                "Exception message should indicate no response is available");
    }
}