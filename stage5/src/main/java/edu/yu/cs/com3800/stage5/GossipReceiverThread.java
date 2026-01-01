package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.ElectionNotification;
import edu.yu.cs.com3800.LeaderElection;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.LoggingServer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

public class GossipReceiverThread extends Thread implements LoggingServer {

    private final PeerServerImpl peerServer;
    private final Long myId;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable;
    private final Logger summaryLogger;
    private final Logger verboseLogger;

    private volatile boolean shutdown = false;

    public GossipReceiverThread(PeerServerImpl server,
                                Logger summaryLogger,
                                Long myId,
                                LinkedBlockingQueue<Message> incomingMessages,
                                LinkedBlockingQueue<Message> outgoingMessages,
                                ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable) throws IOException {
        this.peerServer = server;
        this.myId = myId;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.heartbeatTable = heartbeatTable;

        this.summaryLogger = summaryLogger;
        this.verboseLogger = initializeLogging("GossipVerbose-on-" + myId);

        this.setName("GossipReceiver-" + myId);
        this.setDaemon(true);
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Message msg = incomingMessages.take();

                // Handle GOSSIP messages here
                if (msg.getMessageType() == Message.MessageType.GOSSIP) {
                    handleGossip(msg);
                }
                // If the message is an election message AND leader is still alive then reply with who I think the current leader is
                else if (msg.getMessageType() == Message.MessageType.ELECTION && peerServer.getCurrentLeaderId() != null) {
                    sendElectionResponse(msg);
                }
                else {
                    // Put back non-GOSSIP messages so the main server logic can handle them
                    incomingMessages.offer(msg);
                    Thread.sleep(50); // avoid tight spin loop
                }
            } catch (InterruptedException e) {
                if (shutdown) break;
            } catch (Exception e) {
                summaryLogger.log(Level.SEVERE,
                        "GossipReceiver exception on node " + myId, e);
            }
        }
        verboseLogger.info("GossipReceiverThread for " + myId + " shutting down.");
    }

    private void sendElectionResponse(Message msg) {
        verboseLogger.info("Responding to election message from" +
                msg.getSenderHost() + ":" + msg.getSenderPort()
                + " with current alive leader " + peerServer.getCurrentLeaderId());
        // Send back an ElectionNotification message
        ElectionNotification notification = new ElectionNotification(
                peerServer.getCurrentLeaderId(),
                peerServer.getPeerState(),
                peerServer.getServerId(),
                peerServer.getPeerEpoch()
        );
        // Serialize the ElectionNotification
        byte[] notificationBytes = LeaderElection.buildMsgContent(notification);
        Message response = new Message(
                Message.MessageType.ELECTION,
                notificationBytes,
                peerServer.getAddress().getHostString(),
                peerServer.getUdpPort(),
                msg.getSenderHost(),
                msg.getSenderPort()
        );
        // Add to outgoing messages queue
        outgoingMessages.offer(response);
    }

    public void shutdown() {
        this.shutdown = true;
        this.interrupt();
    }

    void handleGossip(Message msg) {
        long now = System.currentTimeMillis();
        long senderId = peerServer.getServerIdByPort(msg.getSenderPort());

        // Ignore gossip from failed nodes (crash-stop semantics)
        if (this.peerServer.isFailed(senderId)) {
            // Log the ignored gossip
            verboseLogger.info("Ignoring gossip from failed node " + senderId);
            return;
        }

        // Deserialize heartbeat table
        Map<Long, Long> received = deserializeHeartbeatTable(msg.getMessageContents());

        // Verbose logging: entire message
        verboseLogger.info(myId + ": Received gossip from " + senderId +
                " at time " + now +
                " contents=" + received.toString());

        // Merge into my local table
        mergeHeartbeatInfo(senderId, received);
    }

    public Map<Long, Long> deserializeHeartbeatTable(byte[] data) {
        Map<Long, Long> map = new ConcurrentHashMap<>();

        if (data == null || data.length == 0) {
            return map;
        }

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);

        while (buf.remaining() >= 16) {
            long id = buf.getLong();         // server ID
            long heartbeat = buf.getLong();  // heartbeat counter
            map.put(id, heartbeat);
        }

        return map;
    }

    void mergeHeartbeatInfo(long senderId, Map<Long, Long> received) {
        long now = System.currentTimeMillis();

        for (Map.Entry<Long, Long> entry : received.entrySet()) {
            long peerId = entry.getKey();
            long remoteHB = entry.getValue();

            HeartbeatEntry local = heartbeatTable.get(peerId);
            if (local == null) {
                // Check that we aren't adding a failed node back into the table
                if (peerServer.isFailed(peerId)) {
                    continue;
                }

                // New entry!
                local = new HeartbeatEntry(remoteHB, now);
                heartbeatTable.put(peerId, local);

                String msg = myId + ": updated " + peerId
                        + "'s heartbeat sequence to " + remoteHB
                        + " based on message from " + senderId
                        + " at node time " + now;

                summaryLogger.info(msg);
                continue;
            }

            // Ignore all info about failed nodes
            if (local.failed) {
                continue;
            }

            // Vector-clock–style merging: take max heartbeat
            long localHB = local.heartbeat;

            if (remoteHB > localHB) {
                // accept new info!
                local.heartbeat = remoteHB;
                local.lastHeardTime = now;

                String msg = myId + ": updated " + peerId
                        + "'s heartbeat sequence to " + remoteHB
                        + " based on message from " + senderId
                        + " at node time " + now;

                summaryLogger.info(msg);
            }
        }
    }

}