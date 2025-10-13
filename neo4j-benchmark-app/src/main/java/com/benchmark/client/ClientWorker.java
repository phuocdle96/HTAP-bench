package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
                QueryTemplate.PreparedQuery pq = qPool.take(); // your pool stays the same
                long t0 = System.nanoTime();
                try {
                    db.executePrepared(pq); // <-- unified hook (Cypher or Gremlin)
                    long dt = System.nanoTime() - t0;
                    if (!warmup) {
                        results.add(new QueryResult(category, dt));
                    }
                } finally {
                    // recycle
                    qPool.offer(pq);
                }
                if (singleShot) break;
            } while (System.currentTimeMillis() < endWallClockMillis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
