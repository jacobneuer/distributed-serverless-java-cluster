package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class ClientImpl implements Client{

    private final URL endpoint;            // http://<host>:<port>/compileandrun
    private final HttpClient httpClient;   // JDK built-in HTTP client
    private Response lastResponse;         // stored result of the most recent request
    private volatile CompletableFuture<HttpResponse<String>> pending; // track in-flight request


    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        this.endpoint = new URL("http", hostName, hostPort, "/compileandrun");
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        // Send POST request with Java source code in body
        if (src == null) src = "";

        // Convert URL to URI
        final URI uri;
        try {
            uri = endpoint.toURI();
        } catch (URISyntaxException e) {
            throw new IOException("Invalid endpoint URI", e);
        }

        // Build HTTP request
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(src, StandardCharsets.UTF_8))
                .build();

        // Kick off an async request and store it as a future
        pending = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        // Process response when ready
        pending.thenAccept(resp -> {
            this.lastResponse = new Response(resp.statusCode(), resp.body());
        }).exceptionally(ex -> {
            // Store the error as a Response with code 500 and the message
            this.lastResponse = new Response(500, ex.getMessage() == null ? "Unknown error" : ex.getMessage());
            return null;
        });
    }

    @Override
    public Response getResponse() throws IOException {
        // If no request sent yet
        if (pending == null) {
            throw new IOException("No response available: call sendCompileAndRunRequest() first");
        }

        try {
            // Wait up to 2 seconds for completion
            HttpResponse<String> resp = pending.get(2, java.util.concurrent.TimeUnit.SECONDS);
            // Store and return (in case thenAccept didn’t already run)
            if (lastResponse == null) {
                lastResponse = new Response(resp.statusCode(), resp.body());
            }
            return lastResponse;
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IOException("Response not ready within timeout", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for response", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IOException("Error while waiting for response", e.getCause());
        }
    }


}
