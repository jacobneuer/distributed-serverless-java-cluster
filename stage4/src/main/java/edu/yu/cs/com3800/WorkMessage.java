package edu.yu.cs.com3800;

import java.nio.ByteBuffer;

public class WorkMessage {
    private long requestID; // unique identifier for this request
    private byte[] javaCode; // the Java source code to be compiled and run

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + javaCode.length);
        buffer.putLong(requestID);
        buffer.putInt(javaCode.length);
        buffer.put(javaCode);
        return buffer.array();
    }

    public static WorkMessage deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        WorkMessage w = new WorkMessage();
        w.requestID = buffer.getLong();
        int len = buffer.getInt();
        w.javaCode = new byte[len];
        buffer.get(w.javaCode);
        return w;
    }

    public long getRequestID() {
        return requestID;
    }

    public byte[] getJavaCode() {
        return javaCode;
    }
}

