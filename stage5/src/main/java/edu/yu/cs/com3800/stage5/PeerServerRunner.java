package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.Map;

public class PeerServerRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: PeerServerRunner <serverId> <numVotingPeers> <basePort> <udpPort>");
            System.exit(1);
        }

        Long serverId = Long.parseLong(args[0]);
        int numVotingPeers = Integer.parseInt(args[1]);
        int basePort = Integer.parseInt(args[2]);
        int udpPort  = Integer.parseInt(args[3]);

        Map<Long, InetSocketAddress> membership = ClusterConfig.peerMapWithGateway(numVotingPeers, basePort);

        long gatewayId = ClusterConfig.gatewayId(numVotingPeers);

        PeerServerImpl peer = new PeerServerImpl(
                udpPort,
                0L,             // peerEpoch
                serverId,        // THIS node’s ID
                membership,      // includes gateway observer
                gatewayId,       // gateway is a peer ID
                1                // numberOfObservers (gateway)
        );

        peer.start();

        HttpServer server = HttpServer.create(
                new InetSocketAddress(udpPort + 1000), 0
        );

        server.createContext("/status", exchange -> {
            String response =
                    "id=" + peer.getServerId() + "\n" +
                            "state=" + peer.getPeerState() + "\n" +
                            "leader=" + (
                            peer.getCurrentLeader() == null
                                    ? "UNKNOWN"
                                    : peer.getCurrentLeader().getProposedLeaderID()
                    ) + "\n";

            byte[] bytes = response.getBytes();
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });

        server.start();
    }
}