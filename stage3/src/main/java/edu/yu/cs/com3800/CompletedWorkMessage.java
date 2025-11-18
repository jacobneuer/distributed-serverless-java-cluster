package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CompletedWorkMessage {
    private final long requestID;
    private final String codeOutput;      // successful run OR error string
    private final boolean error;

    public CompletedWorkMessage(long requestID, String codeOutput, boolean error) {
        this.requestID = requestID;
        this.codeOutput = codeOutput;
        this.error = error;
    }

    public byte[] serialize() {
        byte[] outBytes = codeOutput.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + Integer.BYTES + outBytes.length);

        buffer.putLong(requestID);
        buffer.put((byte)(error ? 1 : 0));
        buffer.putInt(outBytes.length);
        buffer.put(outBytes);

        return buffer.array();
    }

    public static CompletedWorkMessage deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        long reqID = buffer.getLong();
        boolean error = buffer.get() == 1;

        int len = buffer.getInt();
        byte[] out = new byte[len];
        buffer.get(out);

        String codeOutput = new String(out, StandardCharsets.UTF_8);

        return new CompletedWorkMessage(reqID, codeOutput, error);
    }

    public long getRequestID() {
        return requestID;
    }

    public String getCodeOutput() {
        return codeOutput;
    }

    public boolean getError() {
        return error;
    }
}


