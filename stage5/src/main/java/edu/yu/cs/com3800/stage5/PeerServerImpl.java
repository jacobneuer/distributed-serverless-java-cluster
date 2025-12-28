package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {

    private Logger logger;
    private Logger summaryLogger;

    //Ports
    private final int udpPort;
    private final int tcpPort;

    // UDP Election/Gossip message queues
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;

    // Work Queue (TCP) - Only used when LEADING
    private LinkedBlockingQueue<TCPMessage> tcpWorkQueue;

    private final InetSocketAddress myAddress;
    private ServerState state;
    private volatile boolean shutdown;
    private final Long id;
    private AtomicLong peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private final Map<Integer, Long> portToID;
    private final Long gatewayID;
    private final int numberOfObservers;
    private final int votingPeersCount;
    private final Set<Long> failedNodes;

    // Worker threads
    private TCPServer tcpServer;
    private RoundRobinLeader leaderWorker;
    private JavaRunnerFollower followerWorker;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private final ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable;

    // Gossip + failure detection threads
    private GossipSenderThread gossipSender;
    private GossipReceiverThread gossipReceiver;
    private FailureDetectorThread failureDetector;

    // HTTP server for testing purposes
    private HttpServer httpServer;

    // Completed work cache: requestID -> Message
    private final ConcurrentHashMap<Long, Message> completedWorkCache;
    private final AtomicBoolean recoveryComplete = new AtomicBoolean(false);

    static final int GOSSIP  = 1000;
    static final int FAIL    = GOSSIP * 10;   // 30s
    static final int CLEANUP = FAIL * 2;      // 60s

    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID,
                          Map<Long, InetSocketAddress> peerIDtoAddress,
                          Long gatewayID, int numberOfObservers) throws IOException {
        this.logger = initializeLogging(
                "PeerServerImpl-on-" + serverID + "-on-" + udpPort);
        logger.info("PeerServer " + serverID + " constructed");

        this.summaryLogger = initializeLogging(
                "PeerServerSummary-on-" + serverID + "-on-" + udpPort);

        this.udpPort = udpPort;
        this.tcpPort = udpPort + 2;

        this.myAddress = new InetSocketAddress("localhost", udpPort);

        // Default state is LOOKING
        this.state = ServerState.LOOKING;
        this.shutdown = false;

        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.completedWorkCache = new ConcurrentHashMap<>();

        this.id = serverID;
        this.peerEpoch = new AtomicLong(peerEpoch);
        this.currentLeader = null;
        this.peerIDtoAddress = peerIDtoAddress;

        // Reverse lookup: port → ID
        this.portToID = new HashMap<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            this.portToID.put(entry.getValue().getPort(), entry.getKey());
        }
        // Add myself too
        this.portToID.put(this.udpPort, this.id);

        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;

        // total number of peers (including this one)
        int totalPeers = this.peerIDtoAddress.size();

        // observers do NOT vote
        this.votingPeersCount = totalPeers - numberOfObservers;

        // Initialize the heartbeat table
        this.heartbeatTable = new ConcurrentHashMap<>();
        // Initialize the set of failed nodes
        this.failedNodes = ConcurrentHashMap.newKeySet();

        // Initialize the heartbeat table with all peers
        long now = System.currentTimeMillis();
        // Add myself to the heartbeat table
        heartbeatTable.put(this.id, new HeartbeatEntry(0L, now));
        // Add all peers to the heartbeat table
        for (Long peerId : peerIDtoAddress.keySet()) {
            heartbeatTable.put(peerId, new HeartbeatEntry(0L, now));
        }
    }

    @Override
    public void run() {
        // Start the HTTP status server
        try {
            startHttpStatusServer();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to start HTTP status server", e);
            shutdown = true;
            return;
        }

        // Start the gossip sender thread
        try {
            this.gossipSender = new GossipSenderThread(
                    this,
                    this.id,
                    this.heartbeatTable,
                    this.outgoingMessages,
                    GOSSIP
            );
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to start gossip sending thread", e);
            this.shutdown = true;
            throw new RuntimeException(e);
        }
        this.gossipSender.start();

        // Start the gossip receiver thread
        try {
            this.gossipReceiver = new GossipReceiverThread(
                    this,
                    this.summaryLogger,
                    this.id,
                    this.incomingMessages,
                    this.heartbeatTable
            );
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to start gossip receiving thread", e);
            this.shutdown = true;
            throw new RuntimeException(e);
        }
        this.gossipReceiver.start();

        // Start the failure detector thread
        try {
            this.failureDetector = new FailureDetectorThread(
                    this,
                    this.summaryLogger,
                    this.id,
                    this.heartbeatTable,
                    FAIL,
                    CLEANUP
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.failureDetector.start();

        //Create and run a thread that sends broadcast messages
        try {
            this.senderWorker = new UDPMessageSender(this.outgoingMessages, getUdpPort());
            this.senderWorker.start();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start sender thread", e);
            this.shutdown = true;
            throw new RuntimeException(e);
        }

        //Create and run a thread that listens for messages sent to this server
        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, getAddress(), getUdpPort(), this);
            this.receiverWorker.start();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to start receiver thread", e);
            this.shutdown = true;
            throw new RuntimeException(e);
        }

        //Main server loop
        try {
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        // Run leader election and commit the result
                        LeaderElection leaderElection = new LeaderElection(this, this.incomingMessages, logger);
                        Vote vote = leaderElection.lookForLeader();
                        setCurrentLeader(vote);
                        break;
                    case LEADING:
                        try {
                            // Ensure follower thread is off
                            stopFollowerThread();
                            // Start Leader threads (TCPServer + RoundRobinLeader)
                            if (this.leaderWorker == null) {
                                startLeaderThread();
                            }
                            // Idle lightly
                            Thread.sleep(50);
                        } catch (InterruptedException ie) {
                            this.shutdown = true;
                        }
                        break;
                    case FOLLOWING:
                        // I'm following someone else
                        try {
                            // Ensure leader threads are off
                            stopLeaderThread();
                            // Start Follower thread
                            if (this.followerWorker == null) {
                                startFollowerThread();
                            }
                            // Idle lightly
                            Thread.sleep(50);
                        } catch (InterruptedException ie) {
                            this.shutdown = true;
                        }
                        break;
                    case OBSERVER:
                        if (this.getCurrentLeader() == null) {
                            // If the observer doesn't know who the leader is, start an election
                            LeaderElection election = new LeaderElection(this, incomingMessages, logger);
                            Vote v = election.lookForLeader();
                            setCurrentLeader(v);
                        }

                        // After learning who the leader is just idle and wait for messages (in case of leader death)
                        Thread.sleep(50);
                        break;
                }
            }
        }
        catch (Exception e) {
            // If something unexpected happens in the main loop, log it and shut down
            logger.log(Level.SEVERE, "Uncaught exception in PeerServerImpl main loop for server " + this.id, e);
            this.shutdown = true;
        }
        finally {
            shutdown();
        }
    }

    @Override
    public void shutdown() {
        // Signal to the rest of the server that we are shutting down
        this.shutdown = true;

        // Shutdown the gossip and failure detector threads
        if (gossipSender != null) gossipSender.shutdown();
        if (gossipReceiver != null) gossipReceiver.shutdown();
        if (failureDetector != null) failureDetector.shutdown();

        // Shutdown the leader and follower threads
        stopLeaderThread();
        stopFollowerThread();

        // Ask the sender and receiver threads to stop.
        // These are long-running daemon threads that loop until interrupted.
        if (this.senderWorker != null) {
            this.senderWorker.interrupt();
        }
        if (this.receiverWorker != null) {
            this.receiverWorker.interrupt();
        }

        // Clear queues so we don't keep trying to send after shutdown
        this.outgoingMessages.clear();
        this.incomingMessages.clear();

        // Stop the HTTP status server
        if (httpServer != null) {
            httpServer.stop(0);
        }

        logger.log(Level.INFO, "Server " + this.id + " shutting down.");
    }

    private synchronized void startLeaderThread() {
        // Stop follower thread if running
        stopFollowerThread();

        try {
            // Create the Shared Queue for TCP requests
            this.tcpWorkQueue = new LinkedBlockingQueue<>();

            // Start the TCPServer to listen for new requests
            // Pass the queue so it can deposit requests for the leader to process
            this.tcpServer = new TCPServer(this.tcpPort, this.tcpWorkQueue);
            this.tcpServer.start();

            logger.info("Server " + this.id + " started TCPServer.");

            // Start the recovery pull phase before starting the leader worker
            recoveryPullPhase();

            // Start the RoundRobinLeader to send off requests to workers
            // Pass the same queue so it can process requests from the TCPServer
            this.leaderWorker = new RoundRobinLeader(
                    this,
                    this.tcpWorkQueue,
                    this.peerIDtoAddress,
                    this.id,
                    this.myAddress,
                    this.gatewayID
            );
            this.leaderWorker.start();

            logger.info("Server " + this.id + " started Leader workers (TCPServer + RoundRobinLeader)");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start leader threads", e);
        }
    }

    private synchronized void stopLeaderThread() {
        if (this.leaderWorker != null) {
            this.leaderWorker.interrupt();
            this.leaderWorker = null;
            logger.info("Server " + this.id + ": Stopped RoundRobinLeader thread.");
        }
        if (this.tcpServer != null) {
            this.tcpServer.shutdown();
            this.tcpServer.interrupt();
            this.tcpServer = null;
        }
        this.tcpWorkQueue = null;
    }

    private synchronized void startFollowerThread() throws IOException {
        // Stop the leader thread if running
        stopLeaderThread();

        if (this.followerWorker == null) {
            this.followerWorker = new JavaRunnerFollower(
                    this.udpPort,
                    this.myAddress,
                    this
            );
            this.followerWorker.start();
            logger.info("Server " + this.id + ": Started JavaRunnerFollower thread.");
        }
    }

    private synchronized void stopFollowerThread() {
        if (this.followerWorker != null) {
            this.followerWorker.interrupt();
            this.followerWorker = null;
            logger.info("Server " + this.id + ": Stopped JavaRunnerFollower thread.");
        }
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        // Handle null input
        if (v == null) {
            throw new NullPointerException("Vote cannot be null");
        }

        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        // Handle null input
        if (type == null || messageContents == null || target == null) {
            throw new IllegalArgumentException("Message type, message contents, and target cannot be null");
        }
        // Create the message
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.udpPort, target.getHostString(), target.getPort());
        // Add the message to the outgoing queue
        this.outgoingMessages.add(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        // Check for null input
        if (type == null || messageContents == null) {
            throw new IllegalArgumentException("Message type and message contents cannot be null");
        }
        // Loop through all peers and send the message
        for (InetSocketAddress peer : this.peerIDtoAddress.values()) {
            // If the address is localhost, skip it (don't want to send a message to self)
            if (peer.equals(this.myAddress)) {
                continue;
            }
            try {
                this.sendMessage(type, messageContents, peer);
            } catch (IllegalArgumentException e) {
                logger.log(Level.SEVERE, "Failed to send message to peer " + peer, e);
            }
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        String msg = this.id + ": switching from " + this.state + " to " + newState;

        // If we're switching to LOOKING, turn recovery mode on
        if (newState == ServerState.LOOKING) {
            this.recoveryComplete.set(false);
        }

        System.out.println(msg);
        summaryLogger.info(msg);

        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch.get();
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.udpPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        int aliveVotingPeers = 0;

        for (Long peerId : peerIDtoAddress.keySet()) {
            // Skip observers
            if (peerId.equals(gatewayID)) continue;

            // Skip failed nodes
            if (failedNodes.contains(peerId)) continue;

            aliveVotingPeers++;
        }

        return (aliveVotingPeers / 2) + 1;
    }

    public boolean isFailed(long nodeId) {
        return this.failedNodes.contains(nodeId);
    }

    public void markFailed(long nodeId) {
        this.failedNodes.add(nodeId);
    }

    public Long getServerIdByPort(int port) {
        return this.portToID.get(port);
    }

    public boolean isRecoveryComplete() {
        return this.recoveryComplete.get();
    }

    public void rememberCompletedWork(Long requestId, Message result) {
        this.completedWorkCache.put(requestId, result);
    }

    public Map<Long, Message> getCompletedWorkCache() {
        return this.completedWorkCache;
    }

    private void recoveryPullPhase() {
        logger.info(id + ": starting recovery pull phase");

        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            long peerId = entry.getKey();

            // Skip myself and the gateway
            if (peerId == id || peerId == gatewayID) continue;

            // Skip known-failed nodes
            if (failedNodes.contains(peerId)) continue;

            logger.info("Attempting recovery pull from follower " + entry.getKey());

            InetSocketAddress followerAddr = entry.getValue();
            int followerTcpPort = followerAddr.getPort() + 2;

            boolean recovered = false;

            for (int attempt = 0; attempt < 5 && !recovered; attempt++) {
                try (Socket s = new Socket(followerAddr.getHostString(), followerTcpPort)) {
                    // Send NEW_LEADER_GETTING_LAST_WORK message
                    Message req = new Message(
                            Message.MessageType.NEW_LEADER_GETTING_LAST_WORK,
                            // Empty contents indicate a request for completed work
                            new byte[0],
                            myAddress.getHostString(),
                            tcpPort,
                            followerAddr.getHostString(),
                            followerTcpPort
                    );

                    s.getOutputStream().write(req.getNetworkPayload());
                    s.getOutputStream().flush();
                    s.shutdownOutput();

                    // Time out after some reasonable period
                    s.setSoTimeout(5000);

                    // Read the response
                    byte[] respBytes = Util.readAllBytesFromNetwork(s.getInputStream());
                    if (respBytes.length == 0) continue;

                    Message resp = new Message(respBytes);
                    long gatewayRequestId = resp.getRequestID();

                    if (resp.getMessageType() != Message.MessageType.COMPLETED_WORK) {
                        logger.warning("Unexpected recovery response from " + peerId);
                        continue;
                    }

                    // Check if the request is not -1 (indicates no work to recover)
                    if (gatewayRequestId == -1) {
                        logger.info("No completed work to recover from follower " + peerId);
                        recovered = true;
                        continue;
                    }

                    // Store completed work in the completed work cache
                    Message leaderResponse = new Message(
                            Message.MessageType.COMPLETED_WORK,
                            resp.getMessageContents(),
                            myAddress.getHostString(),
                            tcpPort,
                            null,
                            -1,
                            resp.getRequestID(),
                            resp.getErrorOccurred()
                    );

                    completedWorkCache.put(gatewayRequestId, leaderResponse);
                    recovered = true;
                    logger.info("Recovered completed work for request " + gatewayRequestId);
                } catch (IOException e) {
                    logger.info("Recovery pull from follower " + peerId +
                            " failed (attempt " + attempt + "), retrying...");
                    try { Thread.sleep(200); } catch (InterruptedException ignored) {}
                }
            }
        }

        recoveryComplete.set(true);
        logger.info(id + ": recovery pull phase complete");
    }

    public void reportFailedNode(long failedId) {
        // If we already know this node failed, ignore
        if (failedNodes.contains(failedId)) return;

        // Mark the node as failed
        markFailed(failedId);

        Long leaderId = getCurrentLeaderId();
        boolean leaderFailed = (leaderId != null && leaderId == failedId);

        // If a worker failed, and I'm the leader, handle reassignment
        if (!leaderFailed) {
            handleFailedWorker(failedId);
            return;
        }

        // Leader failed: trigger re-election behavior depending on the role
        summaryLogger.info(id + ": detected leader failure for leader=" + leaderId);

        // Clear current leader
        this.currentLeader = null;

        // Stop any role-specific threads (safe even if null)
        stopLeaderThread();
        stopFollowerThread();

        if (this.state == ServerState.OBSERVER) {
            // Observer doesn't vote, but should re-learn leader
            // Stay OBSERVER; main loop will run observer's lookForLeader() if leader is null
            summaryLogger.info(id + ": observer will re-learn leader");
            return;
        }

        // Increment peer epoch for new election
        this.peerEpoch.incrementAndGet();

        // Voting peer: re-enter LOOKING so the main loop runs election
        setPeerState(ServerState.LOOKING);
    }

    public void handleFailedWorker(long workerId) {
        // If I'm the leader, remove the worker from my leaderWorker and reassign its work
        if (leaderWorker != null) {
            leaderWorker.removeWorker(workerId);
            leaderWorker.reassignWorkFrom(workerId);
        }
    }

    public Long getCurrentLeaderId() {
        Vote v = this.currentLeader;
        return (v == null) ? null : v.getProposedLeaderID();
    }

    private void startHttpStatusServer() throws IOException {
        int httpPort = this.udpPort + 105; // any deterministic offset

        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);

        httpServer.createContext("/leader", exchange -> {
            Vote v = getCurrentLeader();
            String response;

            if (v == null) {
                response = "UNKNOWN";
            } else {
                response = v.getProposedLeaderID() + "," + v.getPeerEpoch();
            }

            exchange.sendResponseHeaders(200, response.getBytes().length);
            exchange.getResponseBody().write(response.getBytes());
            exchange.close();
        });

        httpServer.createContext("/state", exchange -> {
            String response = getPeerState().name();
            exchange.sendResponseHeaders(200, response.length());
            exchange.getResponseBody().write(response.getBytes());
            exchange.close();
        });

        httpServer.start();
        logger.info("Peer " + id + " HTTP status server started on port " + httpPort);
    }

}
