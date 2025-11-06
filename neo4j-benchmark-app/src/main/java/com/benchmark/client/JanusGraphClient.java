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
 * JanusGraph/Gremlin client with optional read-hosts for OLAP/GRAPH load balancing.
 *
 * Usage (via BenchmarkRunner):
 *   --uri gremlin://writer-host:8182
 *   --janus-read-hosts reader1:8182,reader2:8182
 */
public class JanusGraphClient implements DatabaseClient, AutoCloseable {

    private final String writeHost;
    private final int writePort;

    private final List<String> readHosts; // host[:port]; default 8182
    private final int minPoolSize;
    private final int maxPoolSize;
    private final int maxWaitMs;
    private final int queryTimeoutSeconds;

    private Cluster writeCluster;
    private Client  writeClient;

    private Cluster readCluster;   // optional
    private Client  readClient;    // optional

    /** Basic constructor with defaults (single host). */
    public JanusGraphClient(String host, int port) {
        this(host, port, List.of(), 8, 32, 30000, 120);
    }

    public JanusGraphClient(String host,
                            int port,
                            List<String> readHosts,
                            int minPoolSize,
                            int maxPoolSize,
                            int maxWaitMs,
                            int queryTimeoutSeconds) {
        this.writeHost = host;
        this.writePort = port;
        this.readHosts = (readHosts == null) ? List.of() : readHosts;
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.maxWaitMs = maxWaitMs;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    @Override
    public void connect() {
        // build writer cluster
        Cluster.Builder wb = Cluster.build()
                .addContactPoint(writeHost)
                .port(writePort)
                .minConnectionPoolSize(this.minPoolSize)
                .maxConnectionPoolSize(this.maxPoolSize)
                .maxWaitForConnection(this.maxWaitMs)
                .maxContentLength(10 * 1024 * 1024);
        this.writeCluster = wb.create();
        this.writeClient  = writeCluster.connect();

        // build read cluster if provided; else reuse writer
        if (!readHosts.isEmpty()) {
            Cluster.Builder rb = Cluster.build()
                    .minConnectionPoolSize(this.minPoolSize)
                    .maxConnectionPoolSize(this.maxPoolSize)
                    .maxWaitForConnection(this.maxWaitMs)
                    .maxContentLength(10 * 1024 * 1024);

            for (String raw : readHosts) {
                String h = raw.trim();
                if (h.isEmpty()) continue;
                String host = h;
                int port = 8182;
                int idx = h.lastIndexOf(':');
                if (idx > 0 && idx < h.length() - 1) {
                    host = h.substring(0, idx);
                    try { port = Integer.parseInt(h.substring(idx + 1)); } catch (NumberFormatException ignore) {}
                }
                rb.addContactPoint(host).port(port);
            }
            this.readCluster = rb.create();
            this.readClient  = readCluster.connect();
        } else {
            this.readCluster = this.writeCluster;
            this.readClient  = this.writeClient;
        }
    }

    /** Generic GREMLIN submit (writer by default). */
    @Override
    public List<Map<String, Object>> executeQuery(String script, Map<String, Object> params) {
        return submit(writeClient, script, params);
    }

    /** Route GREMLIN prepared queries to read or write client based on readOnly hint. */
    @Override
    public List<Map<String, Object>> executePreparedWithMode(QueryTemplate.PreparedQuery pq, boolean readOnly) {
        if (pq.lang != QueryLanguage.GREMLIN) {
            return DatabaseClient.super.executePreparedWithMode(pq, readOnly);
        }
        Client c = (readOnly ? readClient : writeClient);
        return submit(c, pq.text, pq.params);
    }

    /** Standard GREMLIN prepared execution (writer). */
    @Override
    public List<Map<String, Object>> executePrepared(QueryTemplate.PreparedQuery pq) {
        if (pq.lang == QueryLanguage.GREMLIN) {
            return submit(writeClient, pq.text, pq.params);
        }
        throw new UnsupportedOperationException("JanusGraph client only supports GREMLIN here");
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        // Prefer using writer; sampling is light and avoids extra client pick logic.
        String script = "g.V().has(idProp).hasLabel(label).limit(cap).values(idProp)";
        Map<String, Object> bindings = Map.of("label", label, "idProp", idProperty, "cap", 1_000);

        List<Map<String, Object>> rows = submit(writeClient, script, bindings);
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

    private List<Map<String, Object>> submit(Client client, String script, Map<String, Object> params) {
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

    @Override
    public void close() {
        if (writeClient != null) writeClient.close();
        if (readClient  != null && readClient != writeClient) readClient.close();
        if (writeCluster != null) writeCluster.close();
        if (readCluster  != null && readCluster != writeCluster) readCluster.close();
    }
}
