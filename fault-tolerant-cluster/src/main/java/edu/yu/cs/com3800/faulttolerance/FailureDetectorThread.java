package edu.yu.cs.com3800.faulttolerance;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class FailureDetectorThread extends Thread implements LoggingServer {

    private final PeerServerImpl peerServer;
    private final Long myId;
    private final ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable;
    private final Logger summaryLogger;
    private final Logger verboseLogger;

    private final int FAIL;
    private final int CLEANUP;

    private volatile boolean shutdown = false;

    public FailureDetectorThread(PeerServerImpl peerServer,
                                 Logger summaryLogger,
                                 Long myId,
                                 ConcurrentHashMap<Long, HeartbeatEntry> heartbeatTable,
                                 int FAIL,
                                 int CLEANUP) throws IOException {
        this.peerServer = peerServer;
        this.myId = myId;
        this.heartbeatTable = heartbeatTable;
        this.summaryLogger = summaryLogger;
        this.FAIL = FAIL;
        this.CLEANUP = CLEANUP;

        this.verboseLogger = initializeLogging("FailureDetectorVerbose-on-" + myId);

        this.setName("FailureDetector-" + myId);
        this.setDaemon(true);
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                long now = System.currentTimeMillis();

                for (Map.Entry<Long, HeartbeatEntry> entry : heartbeatTable.entrySet()) {
                    long peerId = entry.getKey();
                    HeartbeatEntry hb = entry.getValue();

                    // Never mark self as failed
                    if (peerId == myId) {
                        continue;
                    }

                    long age = now - hb.lastHeardTime.get();

                    // FAIL: mark node failed
                    if (!hb.failed.get() && age > FAIL) {
                        hb.failed.set(true);

                        String msg = myId + ": no heartbeat from server "
                                + peerId + " - SERVER FAILED";

                        System.out.println(msg);
                        summaryLogger.info(msg);

                        peerServer.reportFailedNode(peerId);
                    }

                    //  CLEANUP: remove entry
                    if (hb.failed.get() && age > CLEANUP) {
                        String msg = myId + ": removing server "
                                + peerId + " from heartbeat table after cleanup";
                        verboseLogger.info(msg);
                        heartbeatTable.remove(peerId);
                    }
                }

                Thread.sleep(FAIL / 5); // check multiple times per FAIL window

            } catch (InterruptedException e) {
                if (shutdown) break;
            } catch (Exception e) {
                summaryLogger.severe(
                        myId + ": exception in FailureDetectorThread: " + e.getMessage()
                );
            }
        }
        verboseLogger.info("FailureDetectorThread for " + myId + " shutting down.");
    }

    public void shutdown() {
        this.shutdown = true;
        this.interrupt();
    }
}