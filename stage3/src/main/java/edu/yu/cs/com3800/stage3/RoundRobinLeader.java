package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread {

    private final Logger logger = Logger.getLogger(RoundRobinLeader.class.getName());

    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final InetSocketAddress myAddress;

    // workers
    private final List<Map.Entry<Long, InetSocketAddress>> workers = new ArrayList<>();
    private int nextWorkerIndex = 0;
    private long requestID = 1;

    // requestID -> client’s address
    private final ConcurrentHashMap<Long, InetSocketAddress> pendingRequests = new ConcurrentHashMap<>();

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
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Wait for the next incoming message
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
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleWorkFromClient(Message clientMsg) {
        try {
            // Generate a unique request ID for this request
            long requestID = generateNextRequestID();

            // Extract raw Java code from the client message
            byte[] javaCode = clientMsg.getMessageContents();

            // Save the client’s address so we can return the result later
            InetSocketAddress clientAddress = new InetSocketAddress(clientMsg.getSenderHost(), clientMsg.getSenderPort());

            // Track this pending request
            pendingRequests.put(requestID, clientAddress);

            // Pick the next worker
            InetSocketAddress workerAddress = pickNextWorker();

            // Forward work to that worker
            Message workerMsg = new Message(
                    Message.MessageType.WORK,
                    javaCode,
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    workerAddress.getHostString(),
                    workerAddress.getPort(),
                    requestID
            );

            // Send work out
            outgoingMessages.add(workerMsg);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleWorkCompletion(Message msg) {
        try {
            // Extract request ID from the message
            long requestID = msg.getRequestID();

            // Look up which client originally sent this request
            InetSocketAddress clientAddr = pendingRequests.remove(requestID);
            if (clientAddr == null) {
                // Log that we got an unknown request ID
                logger.warning("Received COMPLETED_WORK message for unknown request ID: " + requestID);
                return;
            }

            // Worker sends the raw result string in the message contents
            String output = new String(msg.getMessageContents());

            // Build COMPLETED_WORK message to send back to the client
            Message reply = new Message(
                    MessageType.COMPLETED_WORK,
                    output.getBytes(),
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    clientAddr.getHostString(),
                    clientAddr.getPort(),
                    requestID
            );

            // Send the result to the client
            outgoingMessages.add(reply);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized InetSocketAddress pickNextWorker() {
        // Make sure there are workers available
        if (workers.isEmpty()) {
            throw new IllegalStateException("No workers available!");
        }

        // Pick the next worker in round-robin fashion and increment the index
        InetSocketAddress worker = workers.get(nextWorkerIndex).getValue();
        nextWorkerIndex = (nextWorkerIndex + 1) % workers.size();
        return worker;
    }

    private synchronized long generateNextRequestID() {
        return requestID++;
    }
}