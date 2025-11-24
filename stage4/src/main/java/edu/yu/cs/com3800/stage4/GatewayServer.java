package edu.yu.cs.com3800.stage4;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class GatewayServer {

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
                         ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress,
                         int numberOfObservers) throws IOException {

    }

    public GatewayPeerServerImpl getPeerServer() {
        return null;
    }
    
    public void shutdown() {

    }

}
