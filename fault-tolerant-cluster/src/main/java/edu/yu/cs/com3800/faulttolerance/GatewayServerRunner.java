package edu.yu.cs.com3800.faulttolerance;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class GatewayServerRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: GatewayServerRunner <httpPort> <numVotingPeers> <basePort>");
            System.exit(1);
        }

        int httpPort = Integer.parseInt(args[0]);
        int numVotingPeers = Integer.parseInt(args[1]);
        int basePort = Integer.parseInt(args[2]);

        ConcurrentHashMap<Long, InetSocketAddress> membership =
                new ConcurrentHashMap<>(
                        ClusterConfig.peerMapWithGateway(numVotingPeers, basePort)
                );

        long gatewayId = ClusterConfig.gatewayId(numVotingPeers);

        GatewayServer gateway = new GatewayServer(
                httpPort,
                membership.get(gatewayId).getPort(), // gateway UDP port
                0L,
                gatewayId,
                membership,
                1
        );

        gateway.start();
    }
}