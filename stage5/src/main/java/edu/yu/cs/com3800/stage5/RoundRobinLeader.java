package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {

    private final Logger logger;

    private final LinkedBlockingQueue<TCPMessage> tcpWorkQueue;
    private final InetSocketAddress myAddress;

    // Thread pool for handling synchronous TCP connections to workers
    private final ExecutorService workerThreadPool;

    // requestId -> in-flight assignment
    private final ConcurrentHashMap<Long, InFlightRequest> inFlight;

    // failed workers set
    private final Set<Long> failedWorkers;

    // workers
    private final CopyOnWriteArrayList<Map.Entry<Long, InetSocketAddress>> workers;
    private final AtomicInteger nextWorkerIndex;
    private AtomicLong requestID;

    public RoundRobinLeader(
            LinkedBlockingQueue<TCPMessage> tcpWorkQueue,
            Map<Long, InetSocketAddress> peerIDtoAddress,
            Long id,
            InetSocketAddress myAddress,
            Long gatewayID
    ) throws IOException {
        this.logger = initializeLogging(
                "RoundRobinLeader-on-" + id + "-on-" + (myAddress.getPort() + 2));
        logger.info("RoundRobinLeader initialized.");

        this.tcpWorkQueue = tcpWorkQueue;
        this.myAddress = myAddress;
        this.nextWorkerIndex = new AtomicInteger(0);
        this.inFlight = new ConcurrentHashMap<>();
        this.workers = new CopyOnWriteArrayList<>();
        this.requestID = new AtomicLong(1);
        this.failedWorkers = ConcurrentHashMap.newKeySet();

        setDaemon(true);
        setName("RoundRobinLeader-" + id);

        // Initialize Thread Pool
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        this.workerThreadPool = Executors.newFixedThreadPool(poolSize);

        // Build followers list (all peers except myself and the observer)
        for (Map.Entry<Long, InetSocketAddress> e : peerIDtoAddress.entrySet()) {
            if (!Objects.equals(e.getKey(), id) && !Objects.equals(e.getKey(), gatewayID)) {
                logger.info("Adding worker " + e.getKey() + " at " + e.getValue());
                workers.add(e);
            }
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Wait for the next incoming work request
                TCPMessage req = tcpWorkQueue.take();

                logger.info("Leader received request from gateway at port " + req.getMessage().getSenderPort() + ".");

                // Assign the request to a thread in the pool
                // We don't process it here, or we would block the leader
                workerThreadPool.submit(() -> processRequest(req));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.warning("Error in RoundRobinLeader loop: " + e.getMessage());
            }
        }

        // Cleanup on shutdown
        workerThreadPool.shutdownNow();
    }

    private void processRequest(TCPMessage req) {
        Socket gatewaySocket = req.getSocket();
        Message msgFromGateway = req.getMessage();

        // Pick a worker
        Map.Entry<Long, InetSocketAddress> workerEntry = pickNextWorker();
        if (workerEntry == null) {
            logger.severe("No workers available to process request!");
            closeSocket(gatewaySocket);
            return;
        }
        long workerId = workerEntry.getKey();
        InetSocketAddress workerAddress = workerEntry.getValue();

        // Calculate Worker's TCP Port (UDP Port + 2)
        String workerHost = workerAddress.getHostString();
        int workerTcpPort = workerAddress.getPort() + 2;

        logger.info("Picked worker " + workerHost + ":" + workerTcpPort);

        Socket workerSocket = null;
        long currentReqId = -1L;

        boolean requeued = false;
        boolean sentResponseToGateway = false;

        try {
            logger.info("Connecting to worker at " + workerHost + ":" + workerTcpPort);

            // Connect to the Worker via TCP (Leader to Worker)
            workerSocket = new Socket(workerHost, workerTcpPort);

            // Create the Message to send to the Worker
            currentReqId = generateNextRequestID();
            Message workMessage = new Message(
                    MessageType.WORK,
                    msgFromGateway.getMessageContents(),
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    workerHost,
                    workerTcpPort,
                    currentReqId
            );

            logger.info("Sending message from leader to " + workerHost);

            // Send Work to Worker
            OutputStream workerOut = workerSocket.getOutputStream();
            workerOut.write(workMessage.getNetworkPayload());
            workerOut.flush();

            // Store in-flight request
            inFlight.put(currentReqId, new InFlightRequest(req, workerId));

            // Send EOF so the Worker knows we are done writing
            // This allows the Worker's Util.readAllBytesFromNetwork() to return
            workerSocket.shutdownOutput();

            // Read Response from Worker (Blocking)
            InputStream workerIn = workerSocket.getInputStream();
            byte[] resultBytes = Util.readAllBytesFromNetwork(workerIn);
            Message resultMsg = new Message(resultBytes);

            // Check if the worker has failed in the meantime
            // This case occurs when the worker dies AFTER sending response, but before the leader forwards it
            if (failedWorkers.contains(workerId)) {
                logger.warning("Ignoring response from failed worker " + workerId);

                // Remove stale in-flight entry
                inFlight.remove(currentReqId);

                // We do NOT requeue the request here, as we already did so when the failure was detected
                // Return without sending to Gateway
                return;
            }

            logger.info("Received result from worker. Sending to Gateway.");

            // Send Response back to Gateway
            // The resultMsg contents are the actual output string
            // Re-wrap worker response as a LEADER response
            Message leaderResponse = new Message(
                    MessageType.COMPLETED_WORK,
                    resultMsg.getMessageContents(),
                    myAddress.getHostString(),
                    myAddress.getPort() + 2,   // LEADER TCP PORT (8032)
                    msgFromGateway.getSenderHost(),
                    msgFromGateway.getSenderPort(),
                    msgFromGateway.getRequestID(),
                    resultMsg.getErrorOccurred()
            );

            OutputStream gatewayOut = gatewaySocket.getOutputStream();
            gatewayOut.write(leaderResponse.getNetworkPayload());
            gatewayOut.flush();

            sentResponseToGateway = true;

            // Remove request from in-flight requests
            inFlight.remove(currentReqId);
        } catch (Exception e) {
            logger.warning("Worker " + workerId + " failed mid-request, reassigning");

            // Mark worker as failed immediately
            failedWorkers.add(workerId);
            workers.removeIf(w -> w.getKey().equals(workerId));

            // Requeue only if this request was still in-flight
            if (currentReqId != -1L) {
                InFlightRequest removed = inFlight.remove(currentReqId);
                if (removed != null) {
                    tcpWorkQueue.offer(removed.originalRequest);
                    requeued = true;
                }
            }
        } finally {
            // Clean up worker socket
            closeSocket(workerSocket);
            // If we requeued, the new handler needs the same socket.
            if (!requeued && !gatewaySocket.isClosed()) {
                closeSocket(gatewaySocket);
            }
        }
    }


    private Map.Entry<Long, InetSocketAddress> pickNextWorker() {
        // Make sure there are workers available
        if (workers.isEmpty()) return null;

        // Pick the next worker in round-robin fashion and increment the index
        int index = Math.floorMod(nextWorkerIndex.getAndIncrement(), workers.size());
        return workers.get(index);
    }

    private long generateNextRequestID() {
        return requestID.getAndIncrement();
    }

    public void removeWorker(long workerId) {
        failedWorkers.add(workerId);
        workers.removeIf(e -> e.getKey().equals(workerId));

        // Reset round-robin index safely
        nextWorkerIndex.set(0);

        logger.info("Removed failed worker " + workerId);
    }

    public void reassignWorkFrom(long workerId) {
        for (Map.Entry<Long, InFlightRequest> entry : inFlight.entrySet()) {
            InFlightRequest inflight = entry.getValue();

            long assignedWorkerId = inflight.workerId;

            // If this request was assigned to the failed worker, reassign it to another worker
            if (assignedWorkerId == workerId) {
                logger.info("Reassigning request " + entry.getKey() +
                        " from failed worker " + workerId);

                // Remove from in-flight and requeue
                InFlightRequest removed = inFlight.remove(entry.getKey());
                if (removed != null) {
                    tcpWorkQueue.offer(removed.originalRequest);
                }
            }
        }
    }

    private void closeSocket(Socket s) {
        try {
            if (s != null && !s.isClosed()) {
                s.close();
            }
        } catch (IOException e) {
            logger.warning("Error closing socket: " + e.getMessage());
        }
    }

    private static class InFlightRequest {
        final TCPMessage originalRequest;
        final long workerId;

        InFlightRequest(TCPMessage req, long workerId) {
            this.originalRequest = req;
            this.workerId = workerId;
        }
    }

    // Package-private for testing - DELETE LATER
    long getAssignedWorkerForRequest(long requestId) {
        InFlightRequest r = inFlight.get(requestId);
        return (r == null) ? -1L : r.workerId;
    }

    // Package-private for testing - DELETE LATER
    Set<Long> getInFlightRequestIds() {
        return new HashSet<>(inFlight.keySet());
    }

    // Package-private for testing - DELETE LATER
    boolean isFailedWorker(long workerId) {
        return failedWorkers.contains(workerId);
    }

}