package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;

public class JavaRunnerFollower extends Thread {

    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final InetSocketAddress myAddress;

    public JavaRunnerFollower(LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages, Long id, InetSocketAddress myAddress) {
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.myAddress = myAddress;
        setName("JavaRunnerFollower-" + id);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Wait for the next incoming message
                Message msg = incomingMessages.take();

                // Only respond to WORK messages
                if (msg.getMessageType() != MessageType.WORK) {
                    continue;
                }

                // Parse the work payload
                long requestID = msg.getRequestID();
                byte[] javaCode = msg.getMessageContents();

                InetSocketAddress leaderAddress = new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort());

                // Execute the code and send the message result back
                Message response = executeCode(javaCode, requestID, leaderAddress);

                outgoingMessages.add(response);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Message executeCode(byte[] javaCode, long requestID, InetSocketAddress leaderAddress) throws IOException {
        JavaRunner runner = new JavaRunner();

        String resultString;

        try {
            // Convert javaCode into InputStream
            ByteArrayInputStream in = new ByteArrayInputStream(javaCode);

            // JavaRunner.compileAndRun returns a String (SUCCESS)
            resultString = runner.compileAndRun(in);

        } catch (IllegalArgumentException | ReflectiveOperationException e) {
            // JavaRunner signals ALL compilation/runtime errors through exceptions
            resultString = e.getMessage();
        }

        // Build the message to send back to the leader
        return new Message(
                MessageType.COMPLETED_WORK,
                resultString.getBytes(),
                myAddress.getHostString(),
                myAddress.getPort(),
                leaderAddress.getHostString(),
                leaderAddress.getPort(),
                requestID
        );
    }

}
