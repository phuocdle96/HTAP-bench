package com.benchmark.client;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    @Override
    public List<Map<String, Object>> executeQuery(String script, Map<String, Object> params) {
        try {
            Map<String, Object> bindings = (params == null) ? Collections.emptyMap() : params;
            ResultSet rs = client.submit(script, bindings);
            List<Result> all = rs.all().get(60, TimeUnit.SECONDS);

            List<Map<String, Object>> out = new ArrayList<>(all.size());
            for (Result r : all) {
                Object o = r.getObject();
                if (o instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> m = (Map<String, Object>) o;
                    out.add(m);
                } else {
                    out.add(Map.of("value", o));
                }
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("Gremlin submit failed", e);
        }
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        // Gremlin query: g.V().hasLabel(label).values(idProperty).limit(1000)
        String script = "g.V().hasLabel(label).values(idProp).limit(1000)";
        Map<String, Object> params = new HashMap<>();
        params.put("label", label);
        params.put("idProp", idProperty);

        List<Map<String, Object>> rows = executeQuery(script, params);
        // values() typically returns scalars; we added them under key "value" above.
        return rows.stream()
                .map(m -> m.getOrDefault("value", m.get("id")))
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }
}
