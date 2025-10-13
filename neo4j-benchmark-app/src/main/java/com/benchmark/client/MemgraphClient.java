package com.benchmark.client;

import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MemgraphClient implements DatabaseClient, AutoCloseable {

    private final String uri;
    private final String user;
    private final String password;

    private Driver driver;
    private ThreadLocal<Session> tlSession;

    public MemgraphClient(String uri, String user, String password) {
        this.uri = uri;
        this.user = user;
        this.password = password;
    }

    @Override
    public void connect() {
        // Memgraph uses Bolt; no routing flag needed.
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.tlSession = ThreadLocal.withInitial(() ->
                driver.session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .build()));
    }

    @Override
    public List<Map<String, Object>> executeQuery(String cypher, Map<String, Object> params) {
        Session s = tlSession.get();
        Result res = (params == null || params.isEmpty())
                ? s.run(cypher)
                : s.run(cypher, params);

        // Use fully-qualified Record reference to avoid java.lang.Record ambiguity
        return res.list(org.neo4j.driver.Record::asMap);
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        // Adjust label/property for your schema as needed.
        String cypher = "MATCH (n:`" + label + "`) " +
                        "WHERE n.`" + idProperty + "` IS NOT NULL " +
                        "RETURN n.`" + idProperty + "` AS id LIMIT 1000";

        return executeQuery(cypher, Map.of())
                .stream()
                .map(m -> m.get("id"))
                .map(v -> v == null ? null : String.valueOf(v))
                .filter(s -> s != null && !s.isBlank())
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        if (driver != null) driver.close();
    }
}
