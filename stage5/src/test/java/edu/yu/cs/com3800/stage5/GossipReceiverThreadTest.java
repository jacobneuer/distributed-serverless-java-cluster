package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

public class GossipReceiverThreadTest {

    // Helper: serialize map
    private static byte[] serialize(Map<Long, Long> table) {
        ByteBuffer buf = ByteBuffer.allocate(table.size() * 16);
        for (var e : table.entrySet()) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue());
        }
        return buf.array();
    }

    // Minimal PeerServer stub
    private static PeerServerImpl makePeerServerStub(
            Map<Integer, Long> portToId,
            Set<Long> failed) throws Exception {

        return new PeerServerImpl(
                8000,
                0L,
                99L,
                new ConcurrentHashMap<Long, InetSocketAddress>(),
                999L,
                0
        ) {
            @Override
            public Long getServerIdByPort(int port) {
                return portToId.get(port);
            }

            @Override
            public boolean isFailed(long id) {
                return failed.contains(id);
            }
        };
    }

    // TEST: deserialize works
    @Test
    public void deserialize_parsesHeartbeatPairs() throws Exception {
        PeerServerImpl server = makePeerServerStub(Map.of(), Set.of());

        ConcurrentHashMap<Long, HeartbeatEntry> table = new ConcurrentHashMap<>();

        GossipReceiverThread gr = new GossipReceiverThread(
                server,
                Logger.getLogger("summary"),
                1L,
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>(),
                table
        );

        byte[] data = serialize(Map.of(2L, 10L, 3L, 20L));
        Map<Long, Long> out = gr.deserializeHeartbeatTable(data);

        assertEquals(2, out.size());
        assertEquals(10L, out.get(2L));
        assertEquals(20L, out.get(3L));
    }

    // TEST: merge takes max
    @Test
    public void merge_updatesOnlyIfRemoteIsHigher() throws Exception {
        long t0 = System.currentTimeMillis();

        ConcurrentHashMap<Long, HeartbeatEntry> table = new ConcurrentHashMap<>();
        table.put(1L, new HeartbeatEntry(5L, t0));
        table.put(2L, new HeartbeatEntry(7L, t0));

        PeerServerImpl server = makePeerServerStub(Map.of(), Set.of());

        GossipReceiverThread gr = new GossipReceiverThread(
                server,
                Logger.getLogger("summary"),
                99L,
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>(),
                table
        );

        gr.mergeHeartbeatInfo(42L, Map.of(
                1L, 6L,   // higher → update
                2L, 6L    // lower → ignore
        ));

        assertEquals(6L, table.get(1L).heartbeat);
        assertEquals(7L, table.get(2L).heartbeat);
    }

    // TEST: failed entry ignored
    @Test
    public void merge_doesNotUpdateFailedEntry() throws Exception {
        long t0 = System.currentTimeMillis();

        HeartbeatEntry e = new HeartbeatEntry(10L, t0);
        e.failed.set(true);

        ConcurrentHashMap<Long, HeartbeatEntry> table = new ConcurrentHashMap<>();
        table.put(3L, e);

        PeerServerImpl server = makePeerServerStub(Map.of(), Set.of());

        GossipReceiverThread gr = new GossipReceiverThread(
                server,
                Logger.getLogger("summary"),
                99L,
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>(),
                table
        );

        gr.mergeHeartbeatInfo(1L, Map.of(3L, 999L));

        assertEquals(10L, table.get(3L).heartbeat);
    }

    // TEST: failed sender ignored
    @Test
    public void handleGossip_ignoresFailedSender() throws Exception {
        long t0 = System.currentTimeMillis();

        ConcurrentHashMap<Long, HeartbeatEntry> table = new ConcurrentHashMap<>();
        table.put(2L, new HeartbeatEntry(1L, t0));

        Set<Long> failed = ConcurrentHashMap.newKeySet();
        failed.add(7L);

        PeerServerImpl server = makePeerServerStub(
                Map.of(9000, 7L),
                failed
        );

        LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();

        GossipReceiverThread gr = new GossipReceiverThread(
                server,
                Logger.getLogger("summary"),
                99L,
                queue,
                new LinkedBlockingQueue<>(),
                table
        );

        byte[] payload = serialize(Map.of(2L, 999L));

        Message gossip = new Message(
                Message.MessageType.GOSSIP,
                payload,
                "localhost",
                9000,   // sender port → failed node
                "localhost",
                8000
        );

        queue.put(gossip);

        // Simulate one receive cycle
        Message taken = queue.take();
        gr.handleGossip(taken);

        // Should not update because sender is failed
        assertEquals(1L, table.get(2L).heartbeat);
    }
}