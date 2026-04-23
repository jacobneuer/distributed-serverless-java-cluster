package edu.yu.cs.com3800.faulttolerance;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class HeartbeatEntry {
    volatile AtomicLong heartbeat;        // last known heartbeat counter
    volatile AtomicLong lastHeardTime;    // System.currentTimeMillis() when we last heard anything
    volatile AtomicBoolean failed;        // true after we mark this node failed

    public HeartbeatEntry(long initialHeartbeat, long now) {
        this.heartbeat = new AtomicLong(initialHeartbeat);
        this.lastHeardTime = new AtomicLong(now);
        this.failed = new AtomicBoolean(false);
    }
}