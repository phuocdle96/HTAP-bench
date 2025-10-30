// src/main/java/com/benchmark/client/JanusGraphClient.java
package com.benchmark.client;

import com.benchmark.generator.QueryLanguage;
import com.benchmark.generator.QueryTemplate;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class JanusGraphClient implements DatabaseClient, AutoCloseable {

    private final String host;
    private final int port;

    // ✅ ADDED: Tuning parameters
    private final int minPoolSize;
    private final int maxPoolSize;
    private final int maxWaitMs;
    private final int queryTimeoutSeconds;

    private Cluster cluster;
    private Client client;

    /** Basic constructor with defaults (matches original). */
    public JanusGraphClient(String host, int port) {
        this(host, port, 8, 32, 30000, 120); // Sensible defaults
    }

    /**
     * ✅ ADDED: Fully parameterized constructor for tuning.
     */
    public JanusGraphClient(String host, int port, int minPoolSize, int maxPoolSize, int maxWaitMs, int queryTimeoutSeconds) {
        this.host = host;
        this.port = port;
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.maxWaitMs = maxWaitMs;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    @Override
    public void connect() {
        // ✅ CHANGED: Apply all tuning parameters
        this.cluster = Cluster.build()
                .addContactPoint(host)
                .port(port)
                .minConnectionPoolSize(this.minPoolSize)
                .maxConnectionPoolSize(this.maxPoolSize)
                .maxWaitForConnection(this.maxWaitMs)
                .maxContentLength(10 * 1024 * 1024) // 10MB default
                .create();
        this.client = cluster.connect();
    }

    /** Generic GREMLIN submit (used by executePrepared and fetchSampleIds). */
    @Override
    public List<Map<String, Object>> executeQuery(String script, Map<String, Object> params) {
        try {
            // 🛑 REMOVED: `normalizeGremlin` call moved to QueryGenerator
            ResultSet rs = (params == null || params.isEmpty())
                    ? client.submit(script)               // no bindings
                    : client.submit(script, params);      // with bindings

            // Wait for all results (client-side)
            // ✅ CHANGED: Use configurable timeout
            List<Result> all = rs.all().get(this.queryTimeoutSeconds, TimeUnit.SECONDS);

            List<Map<String, Object>> out = new ArrayList<>(all.size());
            for (Result r : all) {
                Object o = r.getObject();
                if (o instanceof Map<?, ?> m) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mm = (Map<String, Object>) m;
                    out.add(mm);
                } else {
                    out.add(Map.of("value", o));
                }
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("Gremlin submit failed: " + e.getMessage(), e);
        }
    }

    /** JanusGraph executes GREMLIN prepared queries. */
    @Override
    public List<Map<String, Object>> executePrepared(QueryTemplate.PreparedQuery pq) {
        if (pq.lang == QueryLanguage.GREMLIN) {
            // `pq.text` is now pre-normalized by QueryGenerator
            return executeQuery(pq.text, pq.params);
        }
        throw new UnsupportedOperationException("JanusGraph client only supports GREMLIN here");
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        // ✅ FIXED SCRIPT (Uses Index)
        String script = "g.V().has(idProp).hasLabel(label).limit(cap).values(idProp)";

        Map<String, Object> bindings = Map.of(
                "label", label,
                "idProp", idProperty,
                "cap", 1_000
        );

        List<Map<String, Object>> rows = executeQuery(script, bindings);
        List<String> out = new ArrayList<>(rows.size());
        for (Map<String, Object> m : rows) {
            Object v = m.get("value"); // This logic is correct!
            if (v instanceof Optional<?> opt) v = opt.orElse(null);
            if (v != null) {
                String s = String.valueOf(v);
                if (!s.isBlank()) out.add(s);
            }
        }
        return out;
    }

    @Override
    public void close() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }
}