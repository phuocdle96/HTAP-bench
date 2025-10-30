package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.Counters;
import com.benchmark.metrics.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class ClientWorker implements Runnable {

    private final DatabaseClient db;
    private final BlockingQueue<QueryTemplate.PreparedQuery> qPool;
    private final String category;
    private final long endWallClockMillis;
    private final boolean warmup;
    private final boolean singleShot;
    private final List<QueryResult> results;
    private final Counters counters;                           // NEW

    private static final ObjectMapper OM = new ObjectMapper();

    public ClientWorker(DatabaseClient db,
                        BlockingQueue<QueryTemplate.PreparedQuery> qPool,
                        String category,
                        long endWallClockMillis,
                        boolean warmup,
                        boolean singleShot,
                        List<QueryResult> results) {
        this(db, qPool, category, endWallClockMillis, warmup, singleShot, results, null);
    }

    public ClientWorker(DatabaseClient db,
                        BlockingQueue<QueryTemplate.PreparedQuery> qPool,
                        String category,
                        long endWallClockMillis,
                        boolean warmup,
                        boolean singleShot,
                        List<QueryResult> results,
                        Counters counters) {                   // NEW
        this.db = db;
        this.qPool = qPool;
        this.category = category;
        this.endWallClockMillis = endWallClockMillis;
        this.warmup = warmup;
        this.singleShot = singleShot;
        this.results = results;
        this.counters = counters;
    }

    @Override
    public void run() {
        try {
            do {
                // Take a template query from the pool
                final QueryTemplate.PreparedQuery template = qPool.take();

                // Up to 4 attempts on retriable errors
                int attempts = 0;
                while (true) {
                    attempts++;

                    // Clone params per execution; ONLY mutate admissionId to avoid unique conflicts.
                    Map<String, Object> execParams = template.params;
                    if (execParams != null && execParams.containsKey("admissionId")) {
                        execParams = new HashMap<>(execParams);
                        execParams.put("admissionId", java.util.UUID.randomUUID().toString());
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
                        if (counters != null) counters.completed.increment();     // NEW
                        break; // success
                    } catch (Throwable t) {
                        String msg = String.valueOf(t.getMessage());

                        // Detect a couple of common retriable cases
                        boolean duplicateId =
                                msg.contains("already exists with label `Event` and property `eventId`")
                                || msg.contains("already exists with label 'Event' and property 'eventId'")
                                || msg.contains("ConstraintValidationFailed");

                        boolean isTransientError =
                                "org.neo4j.driver.exceptions.TransientException".equals(t.getClass().getName());

                        if ((duplicateId || isTransientError) && attempts < 4) {
                            // backoff: 10-50ms scaled by attempt #
                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(10, 50) * attempts);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                            continue; // retry
                        }

                        // --- Enhanced logging: print full query text AND params ---
                        System.err.printf(
                            "[ERR][%s] %s: %s%n" +
                            "   Lang:   %s%n" +
                            "   Query:  %s%n" +
                            "   Params: %s%n",
                            category,
                            t.getClass().getSimpleName(),
                            msg,
                            exec.lang,
                            truncate(exec.text, 4000),
                            toJson(exec.params)
                        );
                        if (counters != null) counters.failed.increment();        // NEW
                        break; // give up on this execution
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

    private static String toJson(Object o) {
        if (o == null) return "null";
        try {
            return OM.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            return String.valueOf(o);
        }
    }
}
