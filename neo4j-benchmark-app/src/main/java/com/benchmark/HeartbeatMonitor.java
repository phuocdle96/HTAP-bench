package com.benchmark;

import com.benchmark.client.DatabaseClient;
import com.benchmark.generator.QueryLanguage;

import java.time.Duration;

/**
 * A silent background heartbeat that pings the DB at a fixed period using a dedicated client.
 *
 * Design goals:
 *  - Fair: identical logical work across engines (no index touches, constant-time).
 *      Neo4j/Memgraph: "RETURN 1"
 *      JanusGraph/Gremlin: "1+1"
 *  - Isolated: separate client/connection pool and its own thread so it cannot steal OLTP workers.
 *  - Silent: no metrics recorded, no console spam unless debug is enabled (we keep it fully silent).
 *  - Bounded: stops automatically at endWallClockMillis.
 */
public final class HeartbeatMonitor implements Runnable, AutoCloseable {

    private final DatabaseClient hbClient;
    private final QueryLanguage lang;
    private final long periodMillis;
    private final long endWallClockMillis;

    private volatile boolean stopped = false;

    public HeartbeatMonitor(DatabaseClient hbClient,
                            QueryLanguage lang,
                            Duration period,
                            long endWallClockMillis) {
        this.hbClient = hbClient;
        this.lang = lang;
        this.periodMillis = Math.max(250L, period.toMillis()); // min 250 ms
        this.endWallClockMillis = endWallClockMillis;
    }

    @Override
    public void run() {
        try {
            hbClient.connect(); // dedicated client/pool

            final String scriptOrCypher = (lang == QueryLanguage.GREMLIN) ? "1+1" : "RETURN 1";

            while (!stopped && System.currentTimeMillis() < endWallClockMillis) {
                // Fire-and-forget: we execute but ignore results, and we swallow exceptions to stay silent
                try {
                    hbClient.executeQuery(scriptOrCypher, java.util.Map.of());
                } catch (Throwable ignored) {
                    // Silent by design. If you ever want to debug, add a gated log here.
                }

                // Sleep until next tick or deadline, whichever is earlier
                final long now = System.currentTimeMillis();
                long sleepMs = Math.min(periodMillis, Math.max(0L, endWallClockMillis - now));
                if (sleepMs <= 0L) break;
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            try { hbClient.close(); } catch (Throwable ignored) {}
        }
    }

    public void stop() { stopped = true; }

    @Override
    public void close() {
        stop();
        try { hbClient.close(); } catch (Throwable ignored) {}
    }
}
