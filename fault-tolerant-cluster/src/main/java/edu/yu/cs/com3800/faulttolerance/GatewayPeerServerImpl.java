package edu.yu.cs.com3800.faulttolerance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends PeerServerImpl {

    public GatewayPeerServerImpl(
            int udpPort, long peerEpoch, Long serverID,
            Map<Long, InetSocketAddress> peerIDtoAddress,
            Long gatewayID, int numberOfObservers) throws IOException {

        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers);
        this.setPeerState(ServerState.OBSERVER);
    }

    @Override
    public void setPeerState(ServerState newState) {
        // Prevent any state transitions as an observer
        super.setPeerState(ServerState.OBSERVER);
    }

}
