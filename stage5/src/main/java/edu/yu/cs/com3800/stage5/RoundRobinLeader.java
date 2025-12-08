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
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {

    private final Logger logger;

    private final LinkedBlockingQueue<TCPMessage> tcpWorkQueue;
    private final InetSocketAddress myAddress;

    // Thread pool for handling synchronous TCP connections to workers
    private final ExecutorService workerThreadPool;

    // workers
    private final List<Map.Entry<Long, InetSocketAddress>> workers = new ArrayList<>();
    private int nextWorkerIndex = 0;
    private long requestID = 1;

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
        InetSocketAddress workerAddress = pickNextWorker();
        if (workerAddress == null) {
            logger.severe("No workers available to process request!");
            closeSocket(gatewaySocket);
            return;
        }

        // Calculate Worker's TCP Port (UDP Port + 2)
        String workerHost = workerAddress.getHostString();
        int workerTcpPort = workerAddress.getPort() + 2;

        logger.info("Picked worker " + workerHost + ":" + workerTcpPort);

        Socket workerSocket = null;

        try {
            logger.info("Connecting to worker at " + workerHost + ":" + workerTcpPort);

            // Connect to the Worker via TCP (Leader to Worker)
            workerSocket = new Socket(workerHost, workerTcpPort);

            // Create the Message to send to the Worker
            long currentReqId = generateNextRequestID();
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

            // Send EOF so the Worker knows we are done writing
            // This allows the Worker's Util.readAllBytesFromNetwork() to return
            workerSocket.shutdownOutput();

            // Read Response from Worker (Blocking)
            InputStream workerIn = workerSocket.getInputStream();
            byte[] resultBytes = Util.readAllBytesFromNetwork(workerIn);
            Message resultMsg = new Message(resultBytes);

            logger.info("Received result from worker. Sending to Gateway.");

            // Send Response back to Gateway
            // The resultMsg contents are the actual output string
            OutputStream gatewayOut = gatewaySocket.getOutputStream();
            gatewayOut.write(resultMsg.getNetworkPayload());
            gatewayOut.flush();
        } catch (Exception e) {
            logger.log(java.util.logging.Level.SEVERE, "Error processing request", e);
        } finally {
            // Clean up gateway and worker sockets
            closeSocket(workerSocket);
            closeSocket(gatewaySocket);
        }
    }


    private synchronized InetSocketAddress pickNextWorker() {
        // Make sure there are workers available
        if (workers.isEmpty()) return null;

        // Pick the next worker in round-robin fashion and increment the index
        InetSocketAddress worker = workers.get(nextWorkerIndex).getValue();
        nextWorkerIndex = (nextWorkerIndex + 1) % workers.size();
        return worker;
    }

    private synchronized long generateNextRequestID() {
        return requestID++;
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
}