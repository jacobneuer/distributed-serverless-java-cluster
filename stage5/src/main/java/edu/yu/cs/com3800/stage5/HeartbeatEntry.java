package edu.yu.cs.com3800.stage5;

public class HeartbeatEntry {
    volatile long heartbeat;        // last known heartbeat counter
    volatile long lastHeardTime;    // System.currentTimeMillis() when we last heard anything
    volatile boolean failed;        // true after we mark this node failed

    public HeartbeatEntry(long initialHeartbeat, long now) {
        this.heartbeat = initialHeartbeat;
        this.lastHeardTime = now;
        this.failed = false;
    }
}