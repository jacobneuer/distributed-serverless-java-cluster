package edu.yu.cs.com3800.gateway;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {

    private Logger logger;

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
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private final Long gatewayID;
    private final int numberOfObservers;
    private final int votingPeersCount;

    // Worker threads
    private TCPServer tcpServer;
    private RoundRobinLeader leaderWorker;
    private JavaRunnerFollower followerWorker;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID,
                          Map<Long, InetSocketAddress> peerIDtoAddress,
                          Long gatewayID, int numberOfObservers) throws IOException {
        this.logger = initializeLogging(
                "PeerServerImpl-on-" + serverID + "-on-" + udpPort);
        logger.info("PeerServer " + serverID + " constructed");

        this.udpPort = udpPort;
        this.tcpPort = udpPort + 2;

        this.myAddress = new InetSocketAddress("localhost", udpPort);

        // Default state is LOOKING
        this.state = ServerState.LOOKING;
        this.shutdown = false;

        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();

        this.id = serverID;
        this.peerEpoch = peerEpoch;
        this.currentLeader = null;
        this.peerIDtoAddress = peerIDtoAddress;

        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;

        // total number of peers (including this one)
        int totalPeers = this.peerIDtoAddress.size();

        // observers do NOT vote
        this.votingPeersCount = totalPeers - numberOfObservers;
    }

    @Override
    public void run() {
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
                        // Update peer epoch
                        this.peerEpoch++;
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
    }

    @Override
    public void shutdown() {
        // Signal to the rest of the server that we are shutting down
        this.shutdown = true;

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

            // Start the RoundRobinLeader to send off requests to workers
            // Pass the same queue so it can process requests from the TCPServer
            this.leaderWorker = new RoundRobinLeader(
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
                    this.myAddress
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
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
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
        return (this.votingPeersCount / 2) + 1;
    }
}
