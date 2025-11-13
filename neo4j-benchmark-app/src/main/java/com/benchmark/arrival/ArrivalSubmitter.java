package com.benchmark.arrival;

import com.benchmark.client.ClientWorker;
import com.benchmark.client.DatabaseClient;
import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/** Poisson arrivals (open-loop). */
public final class ArrivalSubmitter implements Runnable {

    private final double lambda;
    private final ExecutorService workerPool;
    private final DatabaseClient db;
    private final BlockingQueue<QueryTemplate.PreparedQuery> qPool;
    private final String category;

    private final long endMillisWall;
    private final long endNanos;

    private final List<QueryResult> collector;

    public ArrivalSubmitter(double lambda,
                            ExecutorService workerPool,
                            DatabaseClient db,
                            BlockingQueue<QueryTemplate.PreparedQuery> qPool,
                            String category,
                            long endMillisWall,
                            List<QueryResult> collector) {

        this.lambda = lambda;
        this.workerPool = workerPool;
        this.db = db;
        this.qPool = qPool;
        this.category = category;
        this.endMillisWall = endMillisWall;
        this.endNanos = System.nanoTime()
                + (endMillisWall - System.currentTimeMillis()) * 1_000_000L;
        this.collector = collector;

        // NOTE: removed noisy per-shard "will run for ..." logs
    }

    @Override
    public void run() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (System.nanoTime() < endNanos) {
            workerPool.submit(new ClientWorker(
                    db, qPool, category, endMillisWall,
                    /*warm*/ false, /*singleShot*/ true,
                    collector));

            double gapSec  = -Math.log1p(-rnd.nextDouble()) / lambda;
            long   gapNano = (long) (gapSec * 1_000_000_000L);
            LockSupport.parkNanos(gapNano);
        }
    }
}
