package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import java.net.Socket;

/**
 * A container to hold the message received over TCP and the socket
 * it came from, so the RoundRobinLeader can reply to the correct stream.
 */
public class TCPMessage {
    private final Message message;
    private final Socket socket;

    public TCPMessage(Message message, Socket socket) {
        this.message = message;
        this.socket = socket;
    }

    public Message getMessage() {
        return this.message;
    }

    public Socket getSocket() {
        return this.socket;
    }
}
