package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes one query after another until endMillis.
 * In open-loop mode each instance is “single-shot”; in closed mode it
 * can loop as long as tokens are available.
 */
public class ClientWorker implements Runnable {

    private final DatabaseClient                              db;
    private final BlockingQueue<QueryTemplate.PreparedQuery>  qPool;
    private final String                                      category;
    private final long                                        endMillis;
    private final boolean                                     warm;
    private final List<QueryResult>                           collector;
    private final boolean                                     singleShot;

    public ClientWorker(DatabaseClient db,
                        BlockingQueue<QueryTemplate.PreparedQuery> qPool,
                        String category,
                        long endMillis,
                        boolean warm,
                        boolean singleShot,
                        List<QueryResult> collector) {
        this.db         = db;
        this.qPool      = qPool;
        this.category   = category;
        this.endMillis  = endMillis;
        this.warm       = warm;
        this.collector  = collector;
        this.singleShot = singleShot;
    }

    @Override
    public void run() {
        while (System.currentTimeMillis() < endMillis) {
            QueryTemplate.PreparedQuery pq = qPool.poll();
            if (pq == null) break;                          // no work right now

            long start = System.nanoTime();

            /* ---- call the method that exists in Neo4jClient ---- */
            db.executeQuery(pq.cypher(), pq.params());

            long latency = System.nanoTime() - start;

            if (!warm) {
                collector.add(new QueryResult(category, latency));
            }

            qPool.offer(pq);

            /* In open-loop mode this worker is single-shot; exit. */
            if (singleShot) break;
        }
    }
}
