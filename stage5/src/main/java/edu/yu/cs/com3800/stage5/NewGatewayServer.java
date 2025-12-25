package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NewGatewayServer extends Thread implements LoggingServer {

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

    // Cache: requestHash → Message
    private final ConcurrentHashMap<Integer, Message> cache = new ConcurrentHashMap<>();

    // Stage 5 state
    private final BlockingQueue<ClientRequest> pending = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Long, ClientRequest> inFlight = new ConcurrentHashMap<>();

    private volatile Long currentLeaderID = null;
    private volatile long currentLeaderEpoch = -1;
    private volatile boolean leaderAlive = false;

    public NewGatewayServer(int httpPort,
                            int peerPort,
                            long peerEpoch,
                            Long serverID,
                            ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress,
                            int numberOfObservers) throws IOException {

        this.httpPort = httpPort;
        this.peerPort = peerPort;
        this.peerEpoch = peerEpoch;
        this.serverID = serverID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.numberOfObservers = numberOfObservers;

        this.logger = initializeLogging(
                "NewGatewayServer-on-" + serverID + "-on-" + httpPort);

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
            new NewCompileAndRunHandler(
                    exchange,
                    this,
                    cache,
                    requestID.getAndIncrement()
            ).handle();
        });

        // Create /leader endpoint to report who the gateway believes the current leader is
        httpServer.createContext("/leader", exchange -> {
            String response;

            Vote leader = gatewayPeer.getCurrentLeader();

            if (leader == null) {
                response = "UNKNOWN\n";
            } else {
                response = leader.getProposedLeaderID() + "\n";
            }

            byte[] bytes = response.getBytes();
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        });

        httpServer.createContext("/cluster", exchange -> {
            StringBuilder response = new StringBuilder();

            // Leader
            Vote leader = gatewayPeer.getCurrentLeader();
            if (leader == null) {
                response.append("Leader: UNKNOWN\n");
            } else {
                response.append("Leader: ")
                        .append(leader.getProposedLeaderID())
                        .append("\n");
            }

            // Live nodes
            response.append("Live nodes:\n");
            for (Long id : peerIDtoAddress.keySet()) {
                if (!gatewayPeer.isFailed(id)) {
                    response.append(id).append("\n");
                }
            }

            byte[] bytes = response.toString().getBytes();
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
    }

    @Override
    public void run() {
        logger.info("NewGatewayServer thread started.");

        gatewayPeer.start();
        httpServer.start();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                monitorLeader();
                Thread.sleep(200);
            }
        } catch (InterruptedException ignored) {
        } finally {
            shutdown();
        }
    }

    public void enqueueRequest(ClientRequest req) {
        if (!leaderAlive) {
            pending.add(req);
        } else {
            sendToLeader(req);
        }
    }

    private void monitorLeader() {
        Vote leader = gatewayPeer.getCurrentLeader();

        if (leader == null || gatewayPeer.isFailed(leader.getProposedLeaderID())) {
            if (leaderAlive) {
                markLeaderDead();
            }
        } else {
            if (!leaderAlive) {
                markLeaderAlive(leader);
            }
        }
    }

    private void markLeaderDead() {
        logger.warning("Leader marked FAILED");
        leaderAlive = false;
        currentLeaderID = null;
    }

    private void markLeaderAlive(Vote leader) {
        currentLeaderID = leader.getProposedLeaderID();
        currentLeaderEpoch = leader.getPeerEpoch();
        leaderAlive = true;

        logger.info("Leader elected: " + currentLeaderID);
        drainPending();
    }

    private void drainPending() {
        ClientRequest req;
        while ((req = pending.poll()) != null) {
            sendToLeader(req);
        }
    }

    private void sendToLeader(ClientRequest req) {
        inFlight.put(req.requestID, req);

        InetSocketAddress leaderAddr = peerIDtoAddress.get(currentLeaderID);
        int leaderTcpPort = leaderAddr.getPort() + 2;

        try (Socket socket = new Socket(leaderAddr.getHostName(), leaderTcpPort)) {

            Message work = new Message(
                    Message.MessageType.WORK,
                    req.body,
                    httpPort + "",
                    httpPort,
                    leaderAddr.getHostString(),
                    leaderAddr.getPort(),
                    req.requestID
            );

            socket.getOutputStream().write(work.getNetworkPayload());
            socket.shutdownOutput();

            byte[] respBytes = Util.readAllBytesFromNetwork(socket.getInputStream());
            Message resp = new Message(respBytes);

            handleLeaderResponse(resp);

        } catch (Exception e) {
            logger.log(Level.WARNING, "Error sending to leader, requeueing", e);
            inFlight.remove(req.requestID);
            pending.add(req);
        }
    }

    private void handleLeaderResponse(Message resp) {
        ClientRequest req = inFlight.remove(resp.getRequestID());
        if (req == null) return;

        // Validate response is from the current leader
        InetSocketAddress leaderPeerAddr = peerIDtoAddress.get(currentLeaderID);
        int leaderTcpPort = leaderPeerAddr.getPort() + 2;
        boolean fromLeader = resp.getSenderPort() == leaderTcpPort;

        if (!leaderAlive) {
            logger.warning("Ignoring response from invalid leader - no leader alive");
            pending.add(req);
            return;
        }
        if (!fromLeader) {
            logger.warning("Ignoring response from invalid leader - wrong leader sending");
            logger.warning("Expected leader ID: " + currentLeaderID + ", but got response from port: " + resp.getSenderPort());
            pending.add(req);
            return;
        }

        // Valid response
        cache.put(req.requestHash, resp);
        respondToClient(req, resp);
    }

    private void respondToClient(ClientRequest req, Message resp) {
        try {
            HttpExchange ex = req.exchange;
            byte[] body = resp.getMessageContents();
            int status = resp.getErrorOccurred() ? 400 : 200;

            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            ex.getResponseHeaders().add("Cached-Response", "false");
            ex.sendResponseHeaders(status, body.length);

            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to respond to client", e);
        }
    }

    public GatewayPeerServerImpl getPeerServer() {
        return gatewayPeer;
    }

    public void shutdown() {
        logger.info("Shutting down NewGatewayServer");

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