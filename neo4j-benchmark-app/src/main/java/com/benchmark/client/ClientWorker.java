package com.benchmark.client;

import com.benchmark.arrival.ArrivalScheduler;
import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.LockSupport;

/**
 * Executes queries pulled from a BlockingQueue until {@code endTimeMillis}.
 * Optionally sleeps according to an {@link ArrivalScheduler} when running in
 * open-loop mode.
 */
public class ClientWorker implements Callable<List<QueryResult>> {

    private final DatabaseClient dbClient;
    private final BlockingQueue<QueryTemplate.PreparedQuery> queue;
    private final String category;
    private final long endTimeMillis;
    private final boolean isWarmup;
    private final ArrivalScheduler scheduler;   // null ⇒ closed-loop

    /* Closed-loop constructor */
    public ClientWorker(DatabaseClient dbClient,
                        BlockingQueue<QueryTemplate.PreparedQuery> queue,
                        String category,
                        long endTimeMillis,
                        boolean isWarmup) {
        this(dbClient, queue, category, endTimeMillis, isWarmup, null);
    }

    /* Open-loop constructor */
    public ClientWorker(DatabaseClient dbClient,
                        BlockingQueue<QueryTemplate.PreparedQuery> queue,
                        String category,
                        long endTimeMillis,
                        boolean isWarmup,
                        ArrivalScheduler scheduler) {
        this.dbClient      = dbClient;
        this.queue         = queue;
        this.category      = category;
        this.endTimeMillis = endTimeMillis;
        this.isWarmup      = isWarmup;
        this.scheduler     = scheduler;
    }

    @Override
    public List<QueryResult> call() {
        List<QueryResult> results = new ArrayList<>();

        while (System.currentTimeMillis() < endTimeMillis &&
               !Thread.currentThread().isInterrupted()) {

            try {
                if (scheduler != null) {              // open-loop
                    LockSupport.parkNanos(scheduler.nextDelayNanos());
                }

                QueryTemplate.PreparedQuery pq = queue.take();

                long t0 = System.nanoTime();
                try {
                    dbClient.executeQuery(pq.cypher(), pq.params());
                } catch (Exception ex) {              // keep running on error
                    if (!isWarmup)
                        System.err.printf("%s error: %s%n", category, ex.getMessage());
                }
                long latency = System.nanoTime() - t0;

                if (!isWarmup) {
                    results.add(new QueryResult(category, latency));
                }
                queue.offer(pq);                      // recycle
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return results;
    }
}
