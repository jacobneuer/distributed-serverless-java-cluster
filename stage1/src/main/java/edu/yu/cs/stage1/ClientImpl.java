package edu.yu.cs.stage1;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;

public class ClientImpl implements Client{

    private final URL endpoint;            // http://<host>:<port>/compileandrun
    private final HttpClient httpClient;   // JDK built-in HTTP client
    private Response lastResponse;         // stored result of the most recent request

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

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }

        // Store response for retrieval
        this.lastResponse = new Response(response.statusCode(), response.body());
    }

    @Override
    public Response getResponse() throws IOException {
        if (lastResponse == null) {
            throw new IOException("No response available: call sendCompileAndRunRequest() first");
        }
        return this.lastResponse;
    }

}
