package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
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

    private static final ObjectMapper OM = new ObjectMapper();

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
                final QueryTemplate.PreparedQuery template = qPool.take();

                int attempts = 0;
                while (true) {
                    attempts++;

                    Map<String, Object> execParams = template.params;
                    if (execParams != null && execParams.containsKey("admissionId")) {
                        execParams = new HashMap<>(execParams);
                        execParams.put("admissionId", java.util.UUID.randomUUID().toString());
                    }

                    QueryTemplate.PreparedQuery exec =
                            new QueryTemplate.PreparedQuery(template.lang, template.text, execParams,
                                                            template.name, template.category);

                    long t0 = System.nanoTime();
                    try {
                        db.executePrepared(exec);
                        long dt = System.nanoTime() - t0;
                        if (!warmup) results.add(new QueryResult(category, dt));
                        break; // success
                    } catch (Throwable t) {
                        String msg = String.valueOf(t.getMessage());

                        boolean duplicateId =
                                msg.contains("already exists with label `Event` and property `eventId`")
                                || msg.contains("already exists with label 'Event' and property 'eventId'")
                                || msg.contains("ConstraintValidationFailed");

                        boolean isTransientError =
                                "org.neo4j.driver.exceptions.TransientException".equals(t.getClass().getName());

                        if ((duplicateId || isTransientError) && attempts < 4) {
                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(10, 50) * attempts);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                            continue; // retry
                        }

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
                        break;
                    }
                }

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
