package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer implements LoggingServer {

    private final Logger logger;

    private final int httpPort;
    private final int peerPort;
    private final long peerEpoch;
    private final Long serverID;
    private final ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private final int numberOfObservers;

    private final HttpServer httpServer;
    private final GatewayPeerServerImpl gatewayPeer;
    private final ExecutorService httpThreadPool;

    private final AtomicLong requestID = new AtomicLong(1);

    // Shared cache: hash(requestBody) -> Message returned
    private final ConcurrentHashMap<Integer, Message> cache = new ConcurrentHashMap<>();

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
                         ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress,
                         int numberOfObservers) throws IOException {
        this.httpPort = httpPort;
        this.peerPort = peerPort;
        this.peerEpoch = peerEpoch;
        this.serverID = serverID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.numberOfObservers = numberOfObservers;

        this.logger = initializeLogging(
                "GatewayServer-on-" + serverID + "-on-" + httpPort);

        logger.info("Initializing GatewayServer...");

        // Start the observer PeerServer
        gatewayPeer = new GatewayPeerServerImpl(
                peerPort,
                peerEpoch,
                serverID,
                peerIDtoAddress,
                serverID,
                numberOfObservers
        );

        gatewayPeer.setName("GatewayPeerServerImpl-" + serverID);
        gatewayPeer.start();

        // Create the HTTP server
        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);

        // Use a thread pool for HTTP requests
        httpThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        httpServer.setExecutor(httpThreadPool);

        // Register the /compileandrun endpoint
        httpServer.createContext("/compileandrun", exchange -> {
            // Handler MUST be stateless → do not store anything in it
            // Pass all required state via local variables
            new CompileAndRunHandler(
                    exchange,
                    gatewayPeer,
                    cache,
                    requestID.get()
            ).handle();
        });

        // Start the HTTP server
        httpServer.start();

        logger.info("HTTP Server listening on port " + httpPort);
    }

    public GatewayPeerServerImpl getPeerServer() {
        return gatewayPeer;
    }
    
    public void shutdown() {
        logger.info("Shutting down GatewayServer...");

        // Stop the HTTP server immediately (close all handlers)
        try {
            httpServer.stop(0);
            logger.info("HTTP server stopped.");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error stopping HTTP server", e);
        }

        // Shut down HTTP thread pool
        try {
            httpThreadPool.shutdownNow();
            logger.info("HTTP thread pool shut down.");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error shutting down thread pool", e);
        }

        // Stop GatewayPeerServerImpl
        try {
            gatewayPeer.shutdown();
            logger.info("GatewayPeerServerImpl shut down.");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error shutting down gateway peer", e);
        }

        logger.info("GatewayServer shutdown complete.");
    }

}
