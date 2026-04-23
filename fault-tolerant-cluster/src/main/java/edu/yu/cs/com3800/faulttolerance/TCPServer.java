package edu.yu.cs.com3800.faulttolerance;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer{
    private final int tcpPort;
    private final LinkedBlockingQueue<TCPMessage> leaderIncomingWorkQueue;
    private volatile boolean shutdown = false;
    private final Logger logger;

    public TCPServer(int tcpPort, LinkedBlockingQueue<TCPMessage> queue) throws IOException {
        this.tcpPort = tcpPort;
        this.leaderIncomingWorkQueue = queue;
        logger = initializeLogging("TCPServer-on-" + tcpPort);
        setDaemon(true);
        setName("TCPServer-port-" + tcpPort);
        logger.info("TCPServer initialized.");
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
            try {
                logger.info("TCPServer started. Listening on port " + tcpPort);

                while (!shutdown && !Thread.currentThread().isInterrupted()) {
                    Socket gatewaySocket = serverSocket.accept();

                    // Relies on the Gateway calling socket.shutdownOutput()
                    byte[] msgBytes = Util.readAllBytesFromNetwork(gatewaySocket.getInputStream());

                    if (msgBytes.length == 0) {
                        // Just close the socket and ignore this invalid connection
                        logger.warning("Received empty TCP request from " + gatewaySocket.getInetAddress() + ". Closing connection.");
                        gatewaySocket.close();
                        continue;
                    }

                    Message message = new Message(msgBytes);
                    TCPMessage tcpMessage = new TCPMessage(message, gatewaySocket);

                    leaderIncomingWorkQueue.put(tcpMessage);

                    // Not closing the gatewaySocket here.
                    // The RoundRobinLeader needs it to send the response back.
                    logger.fine("Accepted TCP request from " + gatewaySocket.getInetAddress() + ", queued for processing.");
                }
            } catch (IOException e) {
                // When the server socket is closed during shutdown
                if (!this.isInterrupted()) {
                    logger.log(Level.WARNING, "Error accepting/reading TCP connection", e);
                }
            } catch (InterruptedException e) {
                // Handle queue interruption
                Thread.currentThread().interrupt(); // Restore interrupt status
                logger.info("TCPServer thread interrupted during queue put.");
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not start TCPServer on port " + tcpPort, e);
        } finally {
            logger.info("TCPServer shutting down.");
        }
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}
