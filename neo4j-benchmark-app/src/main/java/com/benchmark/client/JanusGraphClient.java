package com.benchmark.client;

import com.benchmark.generator.QueryLanguage;
import com.benchmark.generator.QueryTemplate;
import org.apache.tinkerpop.gremlin.driver.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class JanusGraphClient implements DatabaseClient, AutoCloseable {

    private final String host;
    private final int port;

    private Cluster cluster;
    private Client client;

    public JanusGraphClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void connect() {
        this.cluster = Cluster.build()
                .addContactPoint(host)
                .port(port)
                .create();
        this.client = cluster.connect();
    }

    /** Generic GREMLIN submit (used by executePrepared and fetchSampleIds) */
    @Override
    public List<Map<String, Object>> executeQuery(String script, Map<String, Object> params) {
        try {
            Map<String, Object> bindings = (params == null) ? Map.of() : params;
            ResultSet rs = client.submit(script, bindings);
            List<Result> all = rs.all().get(60, TimeUnit.SECONDS);

            List<Map<String, Object>> out = new ArrayList<>(all.size());
            for (Result r : all) {
                Object o = r.getObject();
                if (o instanceof Map<?,?> m) {
                    @SuppressWarnings("unchecked")
                    Map<String,Object> mm = (Map<String,Object>) m;
                    out.add(mm);
                } else {
                    out.add(Map.of("value", o));
                }
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("Gremlin submit failed", e);
        }
    }

    /** IMPORTANT: JanusGraph must handle GREMLIN prepared queries. */
    @Override
    public List<Map<String, Object>> executePrepared(QueryTemplate.PreparedQuery pq) {
        if (pq.lang == QueryLanguage.GREMLIN) {
            return executeQuery(pq.text, pq.params);
        }
        throw new UnsupportedOperationException("JanusGraph client only supports GREMLIN here");
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        // Keep it simple & capped
        String script = "g.V().hasLabel(label).values(idProp).dedup().limit(cap)";
        Map<String, Object> bindings = Map.of("label", label, "idProp", idProperty, "cap", 1_000);

        List<Map<String, Object>> rows = executeQuery(script, bindings);
        // For scalar values we put them under key "value" in executeQuery
        List<String> out = new ArrayList<>(rows.size());
        for (Map<String,Object> m : rows) {
            Object v = m.get("value");
            if (v != null) out.add(String.valueOf(v));
        }
        return out;
    }

    @Override
    public void close() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }
}
