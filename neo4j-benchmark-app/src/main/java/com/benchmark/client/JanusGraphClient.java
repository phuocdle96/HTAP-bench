package com.benchmark.client;

import com.benchmark.generator.QueryLanguage;
import com.benchmark.generator.QueryTemplate;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;

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

    /** Generic GREMLIN submit (used by executePrepared and fetchSampleIds). */
    @Override
    public List<Map<String, Object>> executeQuery(String script, Map<String, Object> params) {
        try {
            Map<String, Object> bindings = (params == null) ? Collections.emptyMap() : params;
            ResultSet rset = client.submit(script, bindings);

            List<Result> all = rset.all().get(60, TimeUnit.SECONDS);
            List<Map<String, Object>> out = new ArrayList<>(all.size());

            for (Result r : all) {
                Object o = r.getObject();
                // Unwrap Optional if server returns Optional<T>
                if (o instanceof Optional<?> opt) {
                    o = opt.orElse(null);
                }
                if (o instanceof Map<?, ?> mm) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> cast = (Map<String, Object>) mm;
                    out.add(cast);
                } else {
                    out.add(Map.of("value", o));
                }
            }
            return out;
        } catch (ResponseException re) {
            // Print remote error details to help diagnose label/prop issues
            String msg = re.getMessage();
            String remote = (re.getRemoteStackTrace() == null) ? "" : ("\nRemote stack:\n" + re.getRemoteStackTrace());
            throw new RuntimeException("Gremlin submit failed: " + msg + remote, re);
        } catch (Exception e) {
            throw new RuntimeException("Gremlin submit failed", e);
        }
    }

    /** JanusGraph must handle GREMLIN prepared queries. */
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
        Map<String, Object> bindings = Map.of(
                "label", label,
                "idProp", idProperty,
                "cap", 1_000
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
