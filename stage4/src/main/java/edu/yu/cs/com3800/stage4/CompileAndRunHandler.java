package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpExchange;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CompileAndRunHandler {

    private final HttpExchange exchange;
    private final GatewayPeerServerImpl gatewayPeer;
    private final ConcurrentHashMap<Integer, Message> cache;
    private final Long requestID;

    private final Logger logger = Logger.getLogger(CompileAndRunHandler.class.getName());

    public CompileAndRunHandler(HttpExchange exchange,
                                GatewayPeerServerImpl gatewayPeer,
                                ConcurrentHashMap<Integer, Message> cache,
                                Long requestID) {
        this.exchange = exchange;
        this.gatewayPeer = gatewayPeer;
        this.cache = cache;
        this.requestID = requestID;
    }

    public void handle() throws IOException {
        logger.info("Gateway received /compileandrun request (ID=" + requestID + ")");

        // Enforce POST request
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            logger.warning("Invalid HTTP method: " + exchange.getRequestMethod());
            exchange.getResponseHeaders().set("Allow", "POST");
            exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
            return;
        }

        // Enforce Content Type
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.equalsIgnoreCase("text/x-java-source")) {
            logger.warning("Invalid Content-Type: " + contentType);
            exchange.sendResponseHeaders(400, -1); // 400 Bad Request
            return;
        }

        // Read the request body and hash it
        byte[] requestBytes = Util.readAllBytes(exchange.getRequestBody());
        int reqHash = java.util.Arrays.hashCode(requestBytes);
        logger.fine("Request hash = " + reqHash);

        boolean cacheHit = cache.containsKey(reqHash);

        // Cache HIT → return cached value
        if (cacheHit) {
            logger.info("Cache HIT for request ID " + requestID);
            Message cached = cache.get(reqHash);
            byte[] body = cached.getMessageContents();

            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.getResponseHeaders().add("Cached-Response", "true");

            int status = cached.getErrorOccurred() ? 400 : 200;
            sendResponse(status, body);
        }
        else { // Cache MISS
            logger.info("Cache MISS for request ID " + requestID + ", contacting Leader");

            // Need to send WORK to leader via TCP
            long leaderID = gatewayPeer.getCurrentLeader().getProposedLeaderID();
            InetSocketAddress leaderAddress = gatewayPeer.getPeerByID(leaderID);
            if (leaderAddress == null) {
                logger.severe("Leader unknown — cannot process request.");
                sendError(503, "Leader not yet known");
                return;
            }

            Message workMsg = new Message (
                    Message.MessageType.WORK,
                    requestBytes,
                    exchange.getLocalAddress().getHostString(),
                    exchange.getLocalAddress().getPort(),
                    leaderAddress.getHostString(),
                    leaderAddress.getPort(),
                    requestID
            );

            Message replyMsg;

            // Send the message to leader
            int leaderTcpPort = leaderAddress.getPort() + 2;

            try {
                logger.fine("Attempting to send WORK request " + requestID + " to leader at " + leaderTcpPort);
                Socket socket = new Socket(leaderAddress.getHostName(), leaderTcpPort);

                OutputStream out = socket.getOutputStream();
                out.write(workMsg.getNetworkPayload());
                out.flush();
                socket.shutdownOutput();

                InputStream in = socket.getInputStream();
                byte[] resp = Util.readAllBytesFromNetwork(in);

                if (resp.length == 0) {
                    logger.severe("Leader returned EMPTY RESPONSE for request " + requestID);
                    sendError(500, "Empty response received from leader");
                    return;
                }

                replyMsg = new Message(resp);
                logger.fine("Received response for request " + requestID);

                // Cache MISS → store in cache
                cache.put(reqHash, replyMsg);

                // Send response back to client - 400 if an error occurred and 200 otherwise
                int status = replyMsg.getErrorOccurred() ? 400 : 200;
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
                exchange.getResponseHeaders().add("Cached-Response", "false");
                sendResponse(status, replyMsg.getMessageContents());
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while contacting leader", e);
                sendError(500, "Error contacting leader: " + e.getMessage());
            }
        }
    }


    private void sendResponse(int code, byte[] body) throws IOException {
        logger.fine("Sending HTTP " + code + " response (" + body.length + " bytes)");
        exchange.sendResponseHeaders(code, body.length);
        OutputStream os = exchange.getResponseBody();
        os.write(body);
        os.flush();
        os.close();
    }

    private void sendError(int status, String msg) throws IOException {
        logger.warning("Sending ERROR " + status + ": " + msg);
        byte[] err = msg.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(status, err.length);
        OutputStream os = exchange.getResponseBody();
        os.write(err);
        os.flush();
        os.close();
    }

}
