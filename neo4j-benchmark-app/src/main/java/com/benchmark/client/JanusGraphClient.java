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
import java.util.stream.Collectors;

/**
 * JanusGraph Gremlin client.
 *
 * Supports multiple Gremlin Server contact points for load balancing:
 *   --janus-hosts=g1,g2,g3  (port is shared via --uri like gremlin://host:8182)
 *
 * Read-only flag is ignored (Gremlin Server doesn't distinguish per session);
 * load-balancing is handled by the Gremlin driver across contact points.
 */
public class JanusGraphClient implements DatabaseClient, AutoCloseable {

    private final List<String> hosts;
    private final int port;

    private final int minPoolSize;
    private final int maxPoolSize;
    private final int maxWaitMs;
    private final int queryTimeoutSeconds;

    private Cluster cluster;
    private Client client;

    /** Basic constructor with single host (back-compat). */
    public JanusGraphClient(String host, int port) {
        this(List.of(host), port, 8, 32, 30000, 120);
    }

    /** Fully parameterized constructor (single host). */
    public JanusGraphClient(String host, int port, int minPoolSize, int maxPoolSize, int maxWaitMs, int queryTimeoutSeconds) {
        this(List.of(host), port, minPoolSize, maxPoolSize, maxWaitMs, queryTimeoutSeconds);
    }

    /** NEW: Multi-host constructor for load balancing. */
    public JanusGraphClient(List<String> hosts, int port, int minPoolSize, int maxPoolSize, int maxWaitMs, int queryTimeoutSeconds) {
        this.hosts = (hosts == null || hosts.isEmpty())
                ? List.of("localhost")
                : hosts.stream().filter(s -> s != null && !s.isBlank())
                        .map(JanusGraphClient::stripPort)
                        .distinct().collect(Collectors.toList());
        this.port = port;
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.maxWaitMs = maxWaitMs;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    private static String stripPort(String h) {
        int idx = h.indexOf(':');
        return (idx > 0) ? h.substring(0, idx) : h;
    }

    @Override
    public void connect() {
        Cluster.Builder b = Cluster.build()
                .port(port)
                .minConnectionPoolSize(this.minPoolSize)
                .maxConnectionPoolSize(this.maxPoolSize)
                .maxWaitForConnection(this.maxWaitMs)
                .maxContentLength(10 * 1024 * 1024); // 10MB

        for (String h : hosts) b.addContactPoint(h);

        this.cluster = b.create();
        this.client = cluster.connect();
    }

    /** Generic GREMLIN submit (used by executePrepared and fetchSampleIds). */
    @Override
    public List<Map<String, Object>> executeQuery(String script, Map<String, Object> params) {
        try {
            ResultSet rs = (params == null || params.isEmpty())
                    ? client.submit(script)
                    : client.submit(script, params);

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
            return executeQuery(pq.text, pq.params);
        }
        throw new UnsupportedOperationException("JanusGraph client only supports GREMLIN here");
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        // Prefer label + existence predicate; ensure exists index or keep cap small.
        String script = "g.V().hasLabel(label).has(idProp, neq(null)).limit(cap).values(idProp)";

        Map<String, Object> bindings = Map.of(
                "label", label,
                "idProp", idProperty,
                "cap", 10_000
        );

        List<Map<String, Object>> rows = executeQuery(script, bindings);
        List<String> out = new ArrayList<>(rows.size());
        for (Map<String, Object> m : rows) {
            Object v = m.get("value");
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
