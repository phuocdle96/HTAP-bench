package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                // Take a template query from the pool
                final QueryTemplate.PreparedQuery template = qPool.take();

                // Up to 3 attempts if we hit a unique-eventId collision
                int attempts = 0;
                while (true) {
                    attempts++;

                    // Clone params per execution; inject a fresh admissionId when present
                    Map<String, Object> execParams = template.params;
                    boolean mutateId = execParams != null && execParams.containsKey("admissionId");
                    if (mutateId) {
                        execParams = new HashMap<>(execParams);
                        execParams.put("admissionId", java.util.UUID.randomUUID().toString());
                        if (execParams.containsKey("admissionDate")) {
                            execParams.put("admissionDate", LocalDate.now().toString());
                        }
                    }

                    QueryTemplate.PreparedQuery exec =
                            new QueryTemplate.PreparedQuery(template.lang, template.text, execParams);

                    long t0 = System.nanoTime();
                    try {
                        db.executePrepared(exec);
                        long dt = System.nanoTime() - t0;
                        if (!warmup) {
                            results.add(new QueryResult(category, dt));
                        }
                        break; // success
                    } catch (Throwable t) {
                        String msg = String.valueOf(t.getMessage());
                        boolean duplicateId =
                                msg.contains("already exists with label `Event` and property `eventId`")
                                || msg.contains("already exists with label 'Event' and property 'eventId'")
                                || msg.contains("ConstraintValidationFailed");

                        if (duplicateId && attempts < 4) {
                            // Generate a new UUID and retry
                            continue;
                        }

                        System.err.printf("[ERR][%s] %s: %s%n   Query: %s :: %s%n",
                                category, t.getClass().getSimpleName(), msg,
                                exec.lang, truncate(exec.text, 200));
                        break;
                    }
                }

                // Return the template to the pool
                qPool.offer(template);

                if (singleShot) break;
            } while (System.currentTimeMillis() < endWallClockMillis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static String truncate(String s, int max) {
        if (s == null || s.length() <= max) return s;
        return s.substring(0, max) + "...";
    }
}

