package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PeerServerImpl extends Thread implements PeerServer {

    private static final Logger logger = Logger.getLogger(PeerServerImpl.class.getName());

    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress) {
        this.myPort = myPort;
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.state = ServerState.LOOKING;
        this.shutdown = false;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.id = id;
        this.peerEpoch = peerEpoch;
        this.currentLeader = null;
        this.peerIDtoAddress = peerIDtoAddress;
    }

    @Override
    public void run() {
        //Step 1: create and run a thread that sends broadcast messages
        this.senderWorker = new UDPMessageSender(this.outgoingMessages, getUdpPort());
        this.senderWorker.start();
        //Step 2: create and run a thread that listens for messages sent to this server
        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, getAddress(), getUdpPort(), this);
            this.receiverWorker.start();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to start receiver thread", e);
            this.shutdown = true;
            throw new RuntimeException(e);
        }
        //Step 3: main server loop
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
                        // I'm the leader. Stage 2 doesn't require leader behavior, so just avoid spinning hot.
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ie) {
                            // respect shutdown if we're asked to stop
                            this.shutdown = true;
                        }
                        break;
                    case FOLLOWING:
                        // I'm following someone else. Same deal: idle politely.
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ie) {
                            this.shutdown = true;
                        }
                        break;
                }
            }
        }
        catch (Exception e) {
            // If something unexpected happens in the main loop, log it and shut down.
            logger.log(Level.SEVERE, "Uncaught exception in PeerServerImpl main loop for server " + this.id, e);
            this.shutdown = true;
        }
    }

    @Override
    public void shutdown() {
        // Signal to the rest of the server that we are shutting down
        this.shutdown = true;

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

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        // Handle null input
        if (v == null) {
            throw new NullPointerException("Vote cannot be null");
        }

        this.currentLeader = v;

        logger.info(String.format(
                "Server %d recorded current leader as %d (epoch %d); state is now %s",
                this.id,
                v.getProposedLeaderID(),
                v.getPeerEpoch(),
                this.state
        ));
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
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
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
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return (this.peerIDtoAddress.size() / 2) + 1;
    }
}
