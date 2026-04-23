package edu.yu.cs.com3800.faulttolerance;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final int tcpPort;
    private final InetSocketAddress myAddress;
    private final PeerServerImpl peerServer;
    private final Logger logger;
    private final JavaRunner runner;
    private ServerSocket serverSocket;

    public JavaRunnerFollower(int udpPort, InetSocketAddress myAddress, PeerServerImpl peerServer) throws IOException {
        // TCP port is UDP port + 2
        this.tcpPort = udpPort + 2;
        this.myAddress = myAddress;
        this.peerServer = peerServer;
        logger = initializeLogging("JavaRunnerFollower-on-" + myAddress.getPort());

        // We can reuse the same JavaRunner instance (it is stateless/thread-safe enough for this)
        try {
            this.runner = new JavaRunner();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize JavaRunner", e);
        }

        setDaemon(true);
        setName("JavaRunnerFollower-port-" + this.tcpPort);
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(this.tcpPort)) {
            this.serverSocket = serverSocket;
            logger.info("JavaRunnerFollower started. Listening on TCP port " + this.tcpPort);

            while (!this.isInterrupted()) {
                Socket leaderSocket = null;
                try {
                    // Accept connection from Leader
                    leaderSocket = serverSocket.accept();

                    // Read the Work Payload
                    InputStream in = leaderSocket.getInputStream();
                    byte[] payload = Util.readAllBytesFromNetwork(in);

                    if (payload.length == 0) {
                        logger.warning("Received empty payload from leader. Closing connection.");
                        leaderSocket.close();
                        continue;
                    }

                    Message msg = new Message(payload);

                    // Process new Work Logic
                    if (msg.getMessageType() == MessageType.WORK) {
                        handleWork(msg, leaderSocket);
                    }
                    // Leader is asking for old work
                    else if (msg.getMessageType() == MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                        handleRecoveryRequest(msg, leaderSocket);
                    }
                    else {
                        logger.warning("Unknown message type: " + msg.getMessageType());
                    }

                } catch (SocketException e) {
                    // Server socket closed during shutdown
                    if (serverSocket.isClosed() || isInterrupted()) {
                        logger.fine("JavaRunnerFollower shutting down cleanly.");
                        break;
                    } else {
                        logger.log(Level.WARNING, "Socket error", e);
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error processing work connection", e);
                } finally {
                    // Ensure the socket is closed after processing
                    if (leaderSocket != null && !leaderSocket.isClosed()) {
                        try {
                            leaderSocket.close();
                        } catch (Exception ignored) {}
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "JavaRunnerFollower server socket failed", e);
        }
    }

    private void handleWork(Message msg, Socket leaderSocket) {
        logger.info("Received WORK request ID " + msg.getRequestID() + " from Leader at port " + (msg.getSenderPort() + 2));

        byte[] javaCode = msg.getMessageContents();
        WorkResult wr = processCode(javaCode);

        Message response = new Message(
                MessageType.COMPLETED_WORK,
                wr.output.getBytes(StandardCharsets.UTF_8),
                myAddress.getHostString(),
                myAddress.getPort(),
                msg.getSenderHost(),
                msg.getSenderPort() + 2,
                msg.getRequestID(),
                wr.error
        );

        try {
            OutputStream out = leaderSocket.getOutputStream();
            out.write(response.getNetworkPayload());
            out.flush();
            leaderSocket.shutdownOutput();

            logger.info("Successfully sent work from worker " + tcpPort + " back to " + (msg.getSenderPort() + 2));
        } catch (Exception e) {
            // Leader died mid-flight: queue result locally
            logger.info("Leader appears to have failed mid-flight; caching completed work locally for request ID " + msg.getRequestID());
            logger.log(Level.WARNING, "Failed to send COMPLETED_WORK to leader, caching locally", e);
            peerServer.rememberCompletedWork(msg.getRequestID(), response);
        }
    }

    private void handleRecoveryRequest(Message msg, Socket leaderSocket) {
        logger.fine("Received NEW_LEADER_GETTING_LAST_WORK on port " + this.tcpPort);

        // Pick at most one cached result (your assumption)
        Long requestId = null;
        Message cached = null;

        for (Map.Entry<Long, Message> e : peerServer.getCompletedWorkCache().entrySet()) {
            requestId = e.getKey();
            cached = e.getValue();
            break;
        }

        Message response;
        if (cached == null) {
            // No work to recover
            response = new Message(
                    MessageType.COMPLETED_WORK,
                    new byte[0],
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    msg.getSenderHost(),
                    msg.getSenderPort(),
                    -1L,
                    false
            );
            logger.info("No cached work to recover for new leader at port " + this.tcpPort);
        } else {
            // Send the cached COMPLETED_WORK (preserve its contents/error flag)
            response = new Message(
                    MessageType.COMPLETED_WORK,
                    cached.getMessageContents(),
                    myAddress.getHostString(),
                    myAddress.getPort(),
                    msg.getSenderHost(),
                    msg.getSenderPort(),
                    requestId,
                    cached.getErrorOccurred()
            );
            logger.info("Recovering cached work for request ID " + requestId + " to new leader at port " + this.tcpPort);
        }

        try {
            OutputStream out = leaderSocket.getOutputStream();
            out.write(response.getNetworkPayload());
            out.flush();
            leaderSocket.shutdownOutput();

            // Only clear AFTER successful sending
            if (cached != null) {
                peerServer.getCompletedWorkCache().remove(requestId);
            }

        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to respond to recovery request; keeping cached work", e);
            // Do NOT clear cache on failure
        }
    }

    @Override
    public void interrupt() {
        try {
            // Close the server socket to unblock accept()
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close(); // unblocks accept()
            }
        } catch (IOException ignored) {}
        super.interrupt();
    }

    private WorkResult processCode(byte[] javaCode) {
        try {
            ByteArrayInputStream codeStream = new ByteArrayInputStream(javaCode);
            String result = runner.compileAndRun(codeStream);
            return new WorkResult(result, false);
        }
        catch (Exception e) {
            String output = exceptionPayload(e);
            return new WorkResult(output, true);
        }
    }

    private static class WorkResult {
        final String output;
        final boolean error;
        WorkResult(String output, boolean error) {
            this.output = output;
            this.error = error;
        }
    }

    private static String exceptionPayload(Exception e) {
        String errorMessage = (e.getMessage() == null) ? "" : e.getMessage();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(outputStream, true, StandardCharsets.UTF_8)) {
            e.printStackTrace(ps);
        }
        String stack = outputStream.toString(StandardCharsets.UTF_8);
        return errorMessage + "\n" + stack;
    }

}
