package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.FreshnessResult;
import com.benchmark.metrics.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.*;
import java.time.temporal.TemporalAccessor;
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

    // NEW: optional sink for freshness samples
    private final List<FreshnessResult> freshnessSink;

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
                        List<FreshnessResult> freshnessSink) {
        this.db = db;
        this.qPool = qPool;
        this.category = category;
        this.endWallClockMillis = endWallClockMillis;
        this.warmup = warmup;
        this.singleShot = singleShot;
        this.results = results;
        this.freshnessSink = freshnessSink;
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
                        List<Map<String,Object>> rows = db.executePrepared(exec);
                        long dt = System.nanoTime() - t0;
                        if (!warmup) results.add(new QueryResult(category, dt));

                        // If this is the freshness read, compute and store lag
                        if (!warmup && freshnessSink != null && exec.name.startsWith("HB.2")) {
                            long lag = computeFreshnessMillisFromRows(rows);
                            if (lag >= 0) freshnessSink.add(new FreshnessResult(lag));
                        }
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

    /** Extract 'ts' and compute now - ts in milliseconds. */
    private static long computeFreshnessMillisFromRows(List<Map<String,Object>> rows) {
        if (rows == null || rows.isEmpty()) return -1;
        Object v = rows.get(0).getOrDefault("ts", rows.get(0).get("value"));
        if (v == null) return -1;

        long tsMillis;
        if (v instanceof Number n) {
            tsMillis = n.longValue(); // JanusGraph heartbeat (epoch millis)
        } else if (v instanceof TemporalAccessor ta) {
            // Neo4j LocalDateTime (no zone) -> assume UTC
            if (ta instanceof LocalDateTime ldt) {
                tsMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
            } else if (ta instanceof Instant inst) {
                tsMillis = inst.toEpochMilli();
            } else {
                // best effort
                try {
                    tsMillis = Instant.from(ta).toEpochMilli();
                } catch (Exception e) {
                    return -1;
                }
            }
        } else if (v instanceof String s) {
            try {
                tsMillis = Instant.parse(s).toEpochMilli();
            } catch (Exception ex) {
                // try parse LocalDateTime without zone
                try {
                    LocalDateTime ldt = LocalDateTime.parse(s);
                    tsMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                } catch (Exception ex2) {
                    return -1;
                }
            }
        } else {
            return -1;
        }
        return System.currentTimeMillis() - tsMillis;
        }
}
