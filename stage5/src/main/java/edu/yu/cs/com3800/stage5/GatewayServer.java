package edu.yu.cs.com3800.stage5;

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

public class GatewayServer extends Thread implements LoggingServer {

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

        logger.info("Constructing GatewayServer...");

        gatewayPeer = new GatewayPeerServerImpl(
                peerPort,
                peerEpoch,
                serverID,
                peerIDtoAddress,
                serverID,
                numberOfObservers
        );

        gatewayPeer.setName("GatewayPeerServerImpl-" + serverID);

        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);

        httpThreadPool = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2);

        httpServer.setExecutor(httpThreadPool);

        httpServer.createContext("/compileandrun", exchange -> {
            new CompileAndRunHandler(
                    exchange,
                    gatewayPeer,
                    cache,
                    requestID.get()
            ).handle();
        });
    }

    /**
     * The main thread lifecycle.
     * When gateway.start() is called, THIS method runs in its own thread.
     */
    @Override
    public void run() {
        logger.info("GatewayServer thread started.");

        // Start observer peer server (thread)
        gatewayPeer.start();
        logger.info("Started GatewayPeerServerImpl.");

        // Start HTTP server (non-blocking)
        httpServer.start();
        logger.info("HTTP Server listening on port " + httpPort);

        // Run until interrupted
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(200);
            }
        }
        catch (InterruptedException e) {
            logger.info("GatewayServer thread interrupted.");
        }

        shutdown();  // perform clean shutdown
    }

    public GatewayPeerServerImpl getPeerServer() {
        return gatewayPeer;
    }

    /**
     * Cleanly shut down both HTTP and PeerServer threads.
     */
    public void shutdown() {
        logger.info("Shutting down GatewayServer...");

        try {
            httpServer.stop(0);
            logger.info("HTTP server stopped.");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error stopping HTTP server", e);
        }

        try {
            httpThreadPool.shutdownNow();
            logger.info("HTTP thread pool shut down.");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error shutting down thread pool", e);
        }

        try {
            gatewayPeer.shutdown();
            logger.info("GatewayPeerServerImpl shut down.");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error shutting down gateway peer", e);
        }

        logger.info("GatewayServer shutdown complete.");
    }
}