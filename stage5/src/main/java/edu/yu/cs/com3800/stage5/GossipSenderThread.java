package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;

public class GossipSenderThread extends Thread implements LoggingServer {

    private final PeerServerImpl peerServer;
    private final Long myId;
    private final ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final Logger logger;

    private final int GOSSIP_INTERVAL;
    private volatile boolean shutdown = false;

    private final Random random = new Random();

    public GossipSenderThread(PeerServerImpl peerServer,
                              Long myId,
                              ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable,
                              LinkedBlockingQueue<Message> outgoingMessages,
                              int gossipIntervalMs) throws IOException {
        this.peerServer = peerServer;
        this.myId = myId;
        this.heartbeatTable = heartbeatTable;
        this.outgoingMessages = outgoingMessages;
        this.logger = initializeLogging("GossipThread-on-" + myId);
        this.GOSSIP_INTERVAL = gossipIntervalMs;

        this.setName("GossipThread-" + myId);
        this.setDaemon(true);
    }

    @Override
    public void run() {
        logger.info("GossipThread for " + myId + " started.");

        while (!shutdown) {
            try {
                long now = System.currentTimeMillis();

                // Increment this node's heartbeat
                HeartbeatEntry me = heartbeatTable.get(myId);
                if (me != null) {
                    me.heartbeat++;
                    me.lastHeardTime = now;
                }

                // Pick a random peer that is NOT me and NOT failed
                List<Long> candidates = new ArrayList<>();
                for (Map.Entry<Long, HeartbeatEntry> entry : heartbeatTable.entrySet()) {
                    Long id = entry.getKey();
                    HeartbeatEntry hb = entry.getValue();

                    if (!id.equals(myId) && !hb.failed) {
                        candidates.add(id);
                    }
                }

                if (!candidates.isEmpty()) {
                    Long targetId = candidates.get(random.nextInt(candidates.size()));
                    InetSocketAddress target = peerServer.getPeerByID(targetId);

                    if (target != null) {
                        // Serialize our heartbeat table into a GOSSIP message
                        byte[] payload = serializeHeartbeatTable();

                        Message gossip = new Message(
                                Message.MessageType.GOSSIP,
                                payload,
                                "localhost",
                                peerServer.getUdpPort(),
                                target.getHostString(),
                                target.getPort()
                        );

                        outgoingMessages.put(gossip);

                        logger.info(myId + ": GOSSIP → " + targetId +
                                " (table size = " + heartbeatTable.size() + ")");
                    }
                }

                Thread.sleep(GOSSIP_INTERVAL);

            } catch (InterruptedException e) {
                if (shutdown) break;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception in GossipThread for node " + myId, e);
            }
        }

        logger.info("GossipThread for " + myId + " shutting down.");
    }

    public void shutdown() {
        this.shutdown = true;
        this.interrupt();
    }

    private byte[] serializeHeartbeatTable() {
        // 16 bytes per entry: (long id, long heartbeat)
        int size = 16 * heartbeatTable.size();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(size);

        for (Map.Entry<Long, HeartbeatEntry> e : heartbeatTable.entrySet()) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue().heartbeat);
        }

        return buf.array();
    }
}