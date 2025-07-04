package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ClientWorker implements Callable<List<QueryResult>> {

    private final DatabaseClient db;
    private final BlockingQueue<QueryTemplate.PreparedQuery> queries;
    private final BlockingQueue<Object> tokens;     // null ⇒ closed‐loop
    private final String category;
    private final long endMillis;
    private final boolean warmup;

    public ClientWorker(DatabaseClient db,
                        BlockingQueue<QueryTemplate.PreparedQuery> queries,
                        BlockingQueue<Object> tokens,
                        String category,
                        long endMillis,
                        boolean warmup) {
        this.db        = db;
        this.queries   = queries;
        this.tokens    = tokens;
        this.category  = category;
        this.endMillis = endMillis;
        this.warmup    = warmup;
    }

    @Override
    public List<QueryResult> call() {
        List<QueryResult> out = new ArrayList<>();

        while (!Thread.currentThread().isInterrupted() &&
               System.currentTimeMillis() < endMillis) {
            try {
                /* ---------- open-loop throttle ---------- */
                if (tokens != null) {
                    long remaining = endMillis - System.currentTimeMillis();
                    if (remaining <= 0) break;                       // phase over
                    Object tok = tokens.poll(remaining, TimeUnit.MILLISECONDS);
                    if (tok == null) break;                         // timer stopped
                }

                /* ---------- run query ---------- */
                QueryTemplate.PreparedQuery pq = queries.take();

                long t0 = System.nanoTime();
                try {
                    db.executeQuery(pq.cypher(), pq.params());
                } catch (Exception ex) {
                    if (!warmup)
                        System.err.printf("%s error: %s%n", category, ex.getMessage());
                }
                if (!warmup)
                    out.add(new QueryResult(category, System.nanoTime() - t0));

                queries.offer(pq);                                  // recycle
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        return out;
    }
}
