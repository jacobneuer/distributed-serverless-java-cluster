package edu.yu.cs.com3800.faulttolerance;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public final class ClusterConfig {
    private ClusterConfig() {}

    /**
     * @param numVotingPeers number of voting peers (NOT including gateway)
     * @param basePort UDP base port for peer id=1
     * @return a membership map that includes voting peers + 1 gateway observer
     */
    public static Map<Long, InetSocketAddress> peerMapWithGateway(int numVotingPeers, int basePort) {
        return peerMapWithGateway(numVotingPeers, basePort, "localhost");
    }

    public static Map<Long, InetSocketAddress> peerMapWithGateway(int numVotingPeers,
                                                                  int basePort,
                                                                  String host) {
        Map<Long, InetSocketAddress> map = new HashMap<>();

        // voting peers: 1..numVotingPeers
        for (long id = 1; id <= numVotingPeers; id++) {
            map.put(id, new InetSocketAddress(host, basePort + (int)(id - 1)));
        }

        // gateway observer: id = numVotingPeers + 1
        long gatewayId = numVotingPeers + 1L;
        int gatewayUdpPort = basePort + numVotingPeers; // next port after last peer
        map.put(gatewayId, new InetSocketAddress(host, gatewayUdpPort));

        return map;
    }

    public static long gatewayId(int numVotingPeers) {
        return numVotingPeers + 1L;
    }
}