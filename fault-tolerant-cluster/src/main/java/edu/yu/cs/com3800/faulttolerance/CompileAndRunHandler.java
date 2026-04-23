package edu.yu.cs.com3800.faulttolerance;

import com.sun.net.httpserver.HttpExchange;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class CompileAndRunHandler implements LoggingServer {

    private final HttpExchange exchange;
    private final GatewayServer gateway;
    private final ConcurrentHashMap<Integer, Message> cache;
    private final long requestID;

    private final Logger logger;

    public CompileAndRunHandler(HttpExchange exchange,
                                GatewayServer gateway,
                                ConcurrentHashMap<Integer, Message> cache,
                                long requestID) throws IOException {
        this.exchange = exchange;
        this.gateway = gateway;
        this.cache = cache;
        this.requestID = requestID;

        this.logger = initializeLogging(
                "GatewayServer-CompileAndRunHandler-on-" + requestID);
    }

    public void handle() throws IOException {
        logger.info("Gateway received /compileandrun request (ID=" + requestID + ")");

        // Enforce POST request
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            logger.warning("Invalid HTTP method: " + exchange.getRequestMethod());
            exchange.getResponseHeaders().set("Allow", "POST");
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        // Enforce Content Type
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.equalsIgnoreCase("text/x-java-source")) {
            logger.warning("Invalid Content-Type: " + contentType);
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        // Read the request body and hash it
        byte[] requestBytes = Util.readAllBytes(exchange.getRequestBody());
        int reqHash = java.util.Arrays.hashCode(requestBytes);

        // Cache HIT - return cached value immediately
        Message cached = cache.get(reqHash);
        if (cached != null) {
            logger.info("Cache HIT for request ID " + requestID);

            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.getResponseHeaders().add("Cached-Response", "true");

            int status = cached.getErrorOccurred() ? 400 : 200;
            sendResponse(status, cached.getMessageContents());
            return;
        }

        // Cache MISS → DO NOT contact leader here
        logger.info("Cache MISS for request ID " + requestID + " — enqueueing");

        // Do NOT set the Cached-Response header yet - response might happen much later
        ClientRequest req = new ClientRequest(requestID, reqHash, requestBytes, exchange);

        // GatewayServer will either send now (if leader alive) or queue.
        gateway.enqueueRequest(req);

        // IMPORTANT: do NOT close the exchange here and do NOT send the response here
        // The GatewayServer will respond later when it gets a valid leader response.
    }

    // Used only for cache-hit responses (immediate)
    private void sendResponse(int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }
}