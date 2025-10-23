package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ClientWorker implements Runnable {

    private final DatabaseClient db;
    private final BlockingQueue<QueryTemplate.PreparedQuery> qPool;
    private final String category;
    private final long endWallClockMillis;
    private final boolean warmup;
    private final boolean singleShot;
    private final List<QueryResult> results;

    public ClientWorker(DatabaseClient db,
                        BlockingQueue<QueryTemplate.PreparedQuery> qPool,
                        String category,
                        long endWallClockMillis,
                        boolean warmup,
                        boolean singleShot,
                        List<QueryResult> results) {
        this.db = db;
        this.qPool = qPool;
        this.category = category;
        this.endWallClockMillis = endWallClockMillis;
        this.warmup = warmup;
        this.singleShot = singleShot;
        this.results = results;
    }

    @Override
    public void run() {
        try {
            do {
                QueryTemplate.PreparedQuery pq = qPool.take();
                long t0 = System.nanoTime();
                try {
                    db.executePrepared(pq); // Cypher or Gremlin depending on pq.lang and client
                    long dt = System.nanoTime() - t0;
                    if (!warmup) {
                        results.add(new QueryResult(category, dt));
                    }
                } catch (Exception e) {
                    // Make failures visible so categories don't silently disappear
                    String snippet = pq.text.length() > 120 ? pq.text.substring(0, 120) + "..." : pq.text;
                    System.err.println("[ERR][" + category + "] " + e.getClass().getSimpleName() + ": " + e.getMessage());
                    System.err.println("   Query: " + pq.lang + " :: " + snippet);
                } finally {
                    qPool.offer(pq); // recycle
                }
                if (singleShot) break;
            } while (System.currentTimeMillis() < endWallClockMillis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
