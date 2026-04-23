package edu.yu.cs.com3800.faulttolerance;

import com.sun.net.httpserver.HttpExchange;

public class ClientRequest {
    final long requestID;
    final int requestHash;
    final byte[] body;
    final HttpExchange exchange;

    ClientRequest(long id, int hash, byte[] body, HttpExchange ex) {
        this.requestID = id;
        this.requestHash = hash;
        this.body = body;
        this.exchange = ex;
    }
}
