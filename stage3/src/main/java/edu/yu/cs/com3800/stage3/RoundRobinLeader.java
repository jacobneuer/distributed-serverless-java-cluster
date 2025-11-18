package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

public class RoundRobinLeader extends Thread {

    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final InetSocketAddress myAddress;

    // workers
    private final List<Map.Entry<Long, InetSocketAddress>> workers = new ArrayList<>();
    private int nextWorkerIndex = 0;

    // requestID -> pending client callback (to be used later)
    private final ConcurrentHashMap<Long, PendingRequest> pending = new ConcurrentHashMap<>();

    private volatile boolean shutdown = false;

    public RoundRobinLeader(
            LinkedBlockingQueue<Message> incomingMessages,
            LinkedBlockingQueue<Message> outgoingMessages,
            Map<Long, InetSocketAddress> peerIDtoAddress,
            Long id,
            InetSocketAddress myAddress
    ) {
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.myAddress = myAddress;

        setName("RoundRobinLeader-" + id);

        // Build followers list (all peers except myself)
        for (Map.Entry<Long, InetSocketAddress> e : peerIDtoAddress.entrySet()) {
            if (!e.getKey().equals(id)) {
                workers.add(e);
            }
        }
    }

    @Override
    public void run() {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
            try {
                Message msg = incomingMessages.take();

                switch (msg.getMessageType()) {

                    case WORK:
                        handleWorkFromClient(msg);
                        break;

                    case COMPLETED_WORK:
                        handleWorkCompletion(msg);
                        break;

                    default:
                        // ignore gossip, election, etc.
                        break;
                }

            } catch (InterruptedException e) {
                shutdown = true;
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleWorkFromClient(Message clientMsg) {
        try {
            // Deserialize the Java source work
            WorkMessage work = WorkMessage.deserialize(clientMsg.getMessageContents());
            long requestID = work.getRequestID();

            // Determine next worker
            InetSocketAddress workerAddress = pickNextWorker();

            // Build WORK message for that worker
            Message workMsg = new Message(
                    MessageType.WORK,
                    clientMsg.getMessageContents(),
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    workerAddress.getHostString(),
                    workerAddress.getPort(),
                    requestID
            );

            // Save the pending request (HTTP linkage added later)
            pending.put(requestID, new PendingRequest(null));

            // Send it to the outgoing queue
            outgoingMessages.add(workMsg);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void handleWorkCompletion(Message msg) {
        try {
            String output = new String(msg.getMessageContents());

            Message reply = new Message(
                    MessageType.COMPLETED_WORK,
                    output.getBytes(),
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    clientAddr.getHostString(),
                    clientAddr.getPort(),
                    requestID
            );
            outgoingMessages.add(reply);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private InetSocketAddress pickNextWorker() {
        // Make sure there are workers available
        if (workers.isEmpty()) {
            throw new IllegalStateException("No workers available!");
        }

        // Pick the next worker in round-robin fashion and increment the index
        InetSocketAddress worker = workers.get(nextWorkerIndex).getValue();
        nextWorkerIndex = (nextWorkerIndex + 1) % workers.size();
        return worker;
    }

    private static class PendingRequest {
        // add HttpExchange or client session info here later
        public PendingRequest(Object o) { }
    }
}