package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**We are implemeting a simplfied version of the election algorithm. For the complete version which covers all possible scenarios, see <a href="https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913">...</a>
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of the leader election.
     */
    private final static int finalizeWait = 3200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently, 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;

    private final PeerServer server;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Logger logger;

    private long proposedLeader;
    private long proposedEpoch;

    // Tracks the latest vote we've heard from each peer (peerID -> their notification)
    private final Map<Long, ElectionNotification> receivedVotes;

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.logger = logger;

        // when an election starts, each server initially votes for itself
        this.proposedLeader = server.getServerId();
        this.proposedEpoch = server.getPeerEpoch();

        // we'll fill this in as we hear from peers during lookForLeader()
        this.receivedVotes = new java.util.HashMap<>();

        // Record our own initial vote in receivedVotes
        ElectionNotification selfNotification = new ElectionNotification(
                this.proposedLeader,            // I'm proposing myself as leader
                server.getPeerState(),          // my current state (LOOKING)
                server.getServerId(),           // who is sending this notification (me)
                this.proposedEpoch              // current epoch
        );
        this.receivedVotes.put(server.getServerId(), selfNotification);
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        try {
            // 1. Start by announcing our current vote (we initially vote for ourselves)
            sendNotifications();

            // We'll use an exponential-ish backoff if we don't hear from anyone
            long notifsTimeout = 200; // start small (ms)
            long startOfQuorumWait = -1; // when we first thought we had quorum for someone

            while (true) {

                Message incoming;
                try {
                    // 2. Try to get the next incoming election message
                    incoming = this.incomingMessages.poll(notifsTimeout, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    // If we're interrupted, treat it like shutdown of election
                    this.logger.log(Level.WARNING, "Election interrupted for server " + server.getServerId(), ie);
                    return null;
                }

                if (incoming == null) {
                    // 3. We didn't get anything in time.
                    //    - resend our current vote to prompt others
                    //    - back off a bit, but cap it at maxNotificationInterval
                    sendNotifications();
                    notifsTimeout = Math.min(notifsTimeout * 2, maxNotificationInterval);

                    // If we thought we had quorum and were in finalizeWait,
                    // check if enough time has passed to accept that quorum.
                    if (startOfQuorumWait > 0 && (System.currentTimeMillis() - startOfQuorumWait) >= finalizeWait) {

                        // We already decided we have enough votes for proposedLeader.
                        // Build a synthetic ElectionNotification for that leader
                        ElectionNotification winnerNotification = new ElectionNotification(
                                this.proposedLeader,
                                PeerServer.ServerState.LOOKING, // state doesn't really matter here now
                                this.server.getServerId(),
                                this.proposedEpoch
                        );

                        return acceptElectionWinner(winnerNotification);
                    }

                    // go back to top of loop and keep waiting
                    continue;
                }

                // 4. We received a message. Decode it.
                if (incoming.getMessageType() != Message.MessageType.ELECTION) {
                    // ignore non-election messages during election
                    continue;
                }

                ElectionNotification received = getNotificationFromMessage(incoming);

                // Ignore notifications from observers or from non-LOOKING peers:
                // Only LOOKING servers are participating in an active election.
                if (received.getState() != PeerServer.ServerState.LOOKING
                        && received.getState() != PeerServer.ServerState.LEADING
                        && received.getState() != PeerServer.ServerState.FOLLOWING) {
                    // in this simplified version we just won't consider any unknown state
                    continue;
                }

                long theirProposedLeader = received.getProposedLeaderID();
                long theirEpoch = received.getPeerEpoch();
                long senderID = received.getSenderID();

                // 5. Record this sender's current vote in receivedVotes
                this.receivedVotes.put(senderID, received);

                // 6. If the received vote "supersedes" my current proposal, adopt it and rebroadcast.
                if (supersedesCurrentVote(theirProposedLeader, theirEpoch)) {
                    this.proposedLeader = theirProposedLeader;
                    this.proposedEpoch = theirEpoch;

                    // Update our own vote in receivedVotes
                    ElectionNotification selfNotification = new ElectionNotification(
                            this.proposedLeader,            // I'm proposing myself as leader
                            server.getPeerState(),          // my current state (LOOKING)
                            server.getServerId(),           // who is sending this notification (me)
                            this.proposedEpoch              // current epoch
                    );
                    this.receivedVotes.put(server.getServerId(), selfNotification);

                    // rebroadcast our new vote to everyone
                    sendNotifications();
                }

                // 7. Now check if we have enough votes to declare a leader
                Vote currentProposal = new Vote(this.proposedLeader, this.proposedEpoch);

                if (haveEnoughVotes(this.receivedVotes, currentProposal)) {
                    // We *think* we have quorum for proposedLeader.

                    if (startOfQuorumWait < 0) {
                        // First time we detect quorum: start a grace timer.
                        // We wait finalizeWait ms to see if a "better" leader shows up.
                        startOfQuorumWait = System.currentTimeMillis();
                    }

                    // Before we immediately accept, we still keep looping to:
                    // - read any newer/higher votes that might appear
                    // - or run out the finalizeWait timer

                    // But: if finalizeWait already elapsed while we were processing,
                    // we can finalize immediately.
                    long elapsed = System.currentTimeMillis() - startOfQuorumWait;
                    if (elapsed >= finalizeWait) {
                        // Build an ElectionNotification for whoever we think won, so we can
                        // reuse acceptElectionWinner() which expects an ElectionNotification.
                        ElectionNotification winnerNotification = new ElectionNotification(
                                this.proposedLeader,
                                PeerServer.ServerState.LOOKING,
                                this.server.getServerId(),
                                this.proposedEpoch
                        );

                        return acceptElectionWinner(winnerNotification);
                    }
                }
                else {
                    // We don't currently have quorum for our proposedLeader.
                    // Reset quorum finalize wait timer.
                    startOfQuorumWait = -1;
                }

                // 8. Loop continues.
                // We'll keep polling, updating proposal, and checking quorum.
                // The backoff timer should reset when we get activity:
                notifsTimeout = 200;
            }
        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE, "Exception occurred during election; election canceled", e);
            return null;
        }
    }

    private void sendNotifications() {
        try {
            // Build an ElectionNotification for my current proposal
            ElectionNotification notification = new ElectionNotification(
                    this.proposedLeader,
                    PeerServer.ServerState.LOOKING,
                    this.server.getServerId(),
                    this.proposedEpoch
            );

            // Serialize it into bytes for transmission
            byte[] msgBytes = buildMsgContent(notification);

            // Log for visibility
            logger.fine(String.format("Server %d sending election notification: leader=%d, epoch=%d",
                    server.getServerId(), proposedLeader, proposedEpoch));

            // Broadcast to all other peers using UDP
            server.sendBroadcast(Message.MessageType.ELECTION, msgBytes);

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to send election notifications from server " + server.getServerId(), e);
        }
    }


    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        // 1. Determine the new state
        if (n.getProposedLeaderID() == this.server.getServerId()) {
            this.server.setPeerState(PeerServer.ServerState.LEADING);
        } else {
            this.server.setPeerState(PeerServer.ServerState.FOLLOWING);
        }

        // 2. Build a final Vote object representing the elected leader
        Vote finalVote = new Vote(n.getProposedLeaderID(), n.getPeerEpoch());

        // 3. Clear out any leftover messages from the election queue
        this.incomingMessages.clear();

        // 4. Log for clarity
        this.logger.info(String.format(
                "Server %d accepted election winner: leader=%d, epoch=%d, state=%s",
                server.getServerId(), n.getProposedLeaderID(), n.getPeerEpoch(), server.getPeerState()
        ));

        // 5. Return the result so lookForLeader() can pass it back
        return finalVote;
    }

    /*
     * We return true if one of the following two cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        int count = 0;

        long proposedLeaderID = proposal.getProposedLeaderID();
        long proposedEpoch    = proposal.getPeerEpoch();

        // Count how many servers are currently voting for the same leader+epoch as our proposal
        for (ElectionNotification n : votes.values()) {
            if (n.getProposedLeaderID() == proposedLeaderID &&
                    n.getPeerEpoch() == proposedEpoch) {
                count++;
            }
        }

        // Compare against quorum size for this server
        return count >= this.server.getQuorumSize();
    }

    public static ElectionNotification getNotificationFromMessage(Message msg) {
        // We expect only ELECTION messages here
        if (msg.getMessageType() != Message.MessageType.ELECTION) {
            throw new IllegalArgumentException("Cannot build ElectionNotification from non-ELECTION message type: " + msg.getMessageType());
        }

        byte[] contents = msg.getMessageContents();
        ByteBuffer buffer = ByteBuffer.wrap(contents);

        // Read fields in the same order they were written in buildMsgContent()
        long proposedLeaderID = buffer.getLong();  // leader being proposed
        char stateChar = buffer.getChar();         // sender's server state
        long senderID = buffer.getLong();          // which server sent this
        long peerEpoch = buffer.getLong();         // epoch

        // Map the char back to a PeerServer.ServerState
        PeerServer.ServerState state = PeerServer.ServerState.getServerState(stateChar);

        // Now build and return the ElectionNotification
        return new ElectionNotification(
                proposedLeaderID,
                state,
                senderID,
                peerEpoch
        );
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(
                Long.BYTES      // proposedLeaderID
                + Character.BYTES // state as a char
                + Long.BYTES      // senderID
                + Long.BYTES      // peerEpoch
        );

        // 1. proposedLeaderID
        buffer.putLong(notification.getProposedLeaderID());

        // 2. sender state as a single char
        char stateChar = notification.getState().getChar();
        buffer.putChar(stateChar);

        // 3. senderID
        buffer.putLong(notification.getSenderID());

        // 4. peerEpoch
        buffer.putLong(notification.getPeerEpoch());

        return buffer.array();
    }
}