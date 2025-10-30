package com.benchmark.arrival;

import com.benchmark.client.ClientWorker;
import com.benchmark.client.DatabaseClient;
import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.Counters;
import com.benchmark.metrics.QueryResult;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * Generates Poisson arrivals (open-loop). After each exponential gap it
 * submits ONE single-shot ClientWorker to a shared virtual-thread pool.
 *
 * time handling:
 *   • endMillisWall — absolute wall clock deadline (ms since epoch) for workers
 *   • endNanos      — monotonic deadline for this generator’s while-loop
 */
public final class ArrivalSubmitter implements Runnable {

    private final double                      lambda;
    private final ExecutorService             workerPool;
    private final DatabaseClient              db;
    private final BlockingQueue<QueryTemplate.PreparedQuery> qPool;
    private final String                      category;

    private final long endMillisWall;   // wall-clock deadline for workers
    private final long endNanos;        // same moment, in nanoTime domain

    private final List<QueryResult> collector;
    private final Counters counters;    // NEW: counts submissions/completions/failures

    public ArrivalSubmitter(double lambda,
                            ExecutorService workerPool,
                            DatabaseClient db,
                            BlockingQueue<QueryTemplate.PreparedQuery> qPool,
                            String category,
                            long endMillisWall,                     // epoch-ms
                            List<QueryResult> collector,
                            Counters counters) {                    // ← NEW

        this.lambda       = lambda;
        this.workerPool   = workerPool;
        this.db           = db;
        this.qPool        = qPool;
        this.category     = category;

        this.endMillisWall = endMillisWall;
        this.endNanos      = System.nanoTime() +
                             (endMillisWall - System.currentTimeMillis()) * 1_000_000L;

        this.collector    = collector;
        this.counters     = counters;

        System.out.printf("Submitter %s will run for %.2f s%n",
                          category, (endNanos - System.nanoTime()) / 1e9);
    }

    @Override
    public void run() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (System.nanoTime() < endNanos) {

            /* single-shot worker */
            counters.submitted.increment(); // ← count offered request
            workerPool.submit(new ClientWorker(
                    db,
                    qPool,
                    category,
                    endMillisWall,          // correct clock for worker loop
                    /* warm */ false,
                    /* singleShot */ true,
                    collector,
                    counters));             // ← pass counters into worker

            /* exponential gap: gap = −ln(U) / λ   (seconds) */
            double gapSec  = -Math.log1p(-rnd.nextDouble()) / lambda;
            long   gapNano = (long) (gapSec * 1_000_000_000L);
            LockSupport.parkNanos(gapNano);
        }
    }
}
