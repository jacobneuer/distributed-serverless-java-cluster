package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final int tcpPort;
    private final InetSocketAddress myAddress;
    private final Logger logger;
    private final JavaRunner runner;
    private ServerSocket serverSocket;

    public JavaRunnerFollower(int udpPort, InetSocketAddress myAddress) throws IOException {
        // TCP port is UDP port + 2
        this.tcpPort = udpPort + 2;
        this.myAddress = myAddress;
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

                    // Process Logic
                    if (msg.getMessageType() == MessageType.WORK) {
                        logger.fine("Received WORK request ID " + msg.getRequestID() + " from Leader");

                        byte[] javaCode = msg.getMessageContents();
                        WorkResult wr = processCode(javaCode);

                        // Send Response back over the SAME socket
                        Message response = new Message(
                                MessageType.COMPLETED_WORK,
                                wr.output.getBytes(),
                                myAddress.getHostString(),
                                myAddress.getPort(),
                                msg.getSenderHost(),
                                msg.getSenderPort(),
                                msg.getRequestID(),
                                wr.error
                        );

                        OutputStream out = leaderSocket.getOutputStream();
                        out.write(response.getNetworkPayload());
                        out.flush();
                    }
                } catch (SocketException e) {
                    if (isInterrupted()) {
                        logger.fine("JavaRunnerFollower shutting down cleanly.");
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
