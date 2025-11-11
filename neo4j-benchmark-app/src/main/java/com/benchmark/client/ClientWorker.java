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
    private static final int MAX_RETRIES = 8;

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
        final boolean readOnlyCategory =
                "OLAP".equalsIgnoreCase(category) || "GRAPH".equalsIgnoreCase(category);

        try {
            do {
                final QueryTemplate.PreparedQuery template = qPool.take();

                int attempts = 0;
                while (true) {
                    attempts++;

                    Map<String, Object> execParams =
                            ensureUniqueIds(template.text, template.params, "OLTP".equalsIgnoreCase(category));
                    execParams = ensureParamTypes(template.text, execParams);

                    QueryTemplate.PreparedQuery exec =
                            new QueryTemplate.PreparedQuery(template.lang, template.text, execParams,
                                    template.name, template.category);

                    long t0 = System.nanoTime();
                    try {
                        db.executePreparedWithMode(exec, readOnlyCategory);
                        long dt = System.nanoTime() - t0;
                        if (!warmup) results.add(new QueryResult(category, dt));
                        break; // success
                    } catch (Throwable t) {
                        String msg = String.valueOf(t.getMessage());

                        boolean duplicateId =
                                msg.contains("already exists with label `Event` and property `eventId`")
                                || msg.contains("already exists with label 'Event' and property 'eventId'")
                                || msg.contains("ConstraintValidationFailed");

                        // Treat Memgraph’s wording as transient too:
                        boolean looksMemgraphConflict =
                                msg.contains("Cannot resolve conflicting transactions");

                        boolean isTransientError =
                                looksMemgraphConflict
                                || (t instanceof org.neo4j.driver.exceptions.TransientException)
                                || (t.getCause() instanceof org.neo4j.driver.exceptions.TransientException)
                                || msg.contains("TransientError")
                                || msg.contains("DeadlockDetected")
                                || msg.contains("ServiceUnavailable");

                        if ((duplicateId || isTransientError) && attempts < MAX_RETRIES) {
                            // jittered exponential backoff (caps quickly)
                            long sleepMs = Math.min(200L, (1L << Math.min(attempts, 10)) + ThreadLocalRandom.current().nextLong(5, 25));
                            try { Thread.sleep(sleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
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

    /** Ensure numeric params (limit/skip/etc.) are integers, even if the OPEN sampler handed us strings. */
    private static Map<String, Object> ensureParamTypes(String query, Map<String, Object> in) {
        if (in == null || in.isEmpty()) return in;

        Map<String, Object> p = new HashMap<>(in);

        // Names that must be integers in Cypher (LIMIT/SKIP, hop counts, etc.)
        List<String> intNames = Arrays.asList(
                "limit", "skip", "offset", "topN", "topK", "k", "hops", "degree",
                "minCount", "maxCount", "numSamples", "take", "sample"
        );

        for (String name : intNames) {
            if (p.containsKey(name)) {
                p.put(name, coerceToInt(p.get(name), defaultFor(name)));
            }
        }

        // If your queries use different param labels, add them here or derive from the query text.

        return p;
    }

    private static int defaultFor(String name) {
        switch (name) {
            case "limit": return 25;
            case "skip":
            case "offset": return 0;
            case "hops":
            case "degree":
            case "k":
            case "topK": return 3;
            case "topN": return 10;
            case "minCount": return 1;
            case "maxCount": return 100;
            case "numSamples":
            case "take":
            case "sample": return 50;
            default: return 10;
        }
    }

    private static int coerceToInt(Object v, int defVal) {
        try {
            if (v == null) return defVal;
            if (v instanceof Number) return ((Number) v).intValue();
            if (v instanceof String) {
                String s = ((String) v).trim();
                // Fast path: digits only
                if (s.matches("^-?\\d+$")) return Integer.parseInt(s);
                // Try to peel leading non-digits like "X12345"
                String digits = s.replaceAll(".*?(-?\\d+).*", "$1");
                if (digits.matches("^-?\\d+$")) return Integer.parseInt(digits);
            }
        } catch (Exception ignore) { /* fallthrough */ }
        return defVal;
    }

    /** Make per-execution UUIDs for CREATE-time IDs so we don’t collide when reusing templates. */
    private static Map<String,Object> ensureUniqueIds(String query, Map<String,Object> in, boolean isOLTP) {
        if (!isOLTP || in == null || in.isEmpty()) return in;

        String q = (query == null ? "" : query).toUpperCase(Locale.ROOT);
        boolean hasCreate = q.contains("CREATE ");
        if (!hasCreate) return in;

        Map<String,Object> p = new HashMap<>(in);

        java.util.function.Predicate<String> uses = param ->
                q.contains("$" + param.toUpperCase(Locale.ROOT));

        if (uses.test("eventId")) {
            p.put("eventId", UUID.randomUUID().toString());
        }
        if (uses.test("admissionId") && q.contains(":ADMISSION")) {
            p.put("admissionId", UUID.randomUUID().toString());
        }
        if (uses.test("dischargeId") && q.contains(":DISCHARGE")) {
            p.put("dischargeId", UUID.randomUUID().toString());
        }
        if (uses.test("procedureId")) {
            p.put("procedureId", UUID.randomUUID().toString());
        }
        if (uses.test("chemoId")) {
            p.put("chemoId", UUID.randomUUID().toString());
        }
        if (uses.test("newAdmissionId")) {
            p.put("newAdmissionId", UUID.randomUUID().toString());
        }
        if (uses.test("newEventId")) {
            p.put("newEventId", java.util.UUID.randomUUID().toString());
        }

        // Do NOT modify reference IDs: patientId, unitId, diagnosisCode, currentAdmissionId, etc.
        return p;
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


