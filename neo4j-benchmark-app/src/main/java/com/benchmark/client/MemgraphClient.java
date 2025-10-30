package com.benchmark.client;

import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MemgraphClient implements DatabaseClient, AutoCloseable {

    private final Driver driver;

    public MemgraphClient(String uri, String user, String password) {
        // Memgraph uses Bolt/Neo4j driver. Empty user/password is allowed if server permits it.
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user == null ? "" : user,
                                                                 password == null ? "" : password));
    }

    @Override
    public void connect() { /* driver created in ctor */ }

    @Override
    public List<Map<String, Object>> executeQuery(String cypher, Map<String, Object> params) {
        try (Session s = driver.session(SessionConfig.builder()
                .withDefaultAccessMode(AccessMode.WRITE) // most workloads write or read-write; adjust if needed
                .build())) {
            Result res = (params == null || params.isEmpty()) ? s.run(cypher) : s.run(cypher, params);
            return res.list(org.neo4j.driver.Record::asMap);
        }
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        String cypher = "MATCH (n:`" + label + "`) " +
                        "WHERE n.`" + idProperty + "` IS NOT NULL " +
                        "RETURN n.`" + idProperty + "` AS id LIMIT $cap";
        try (Session s = driver.session()) {
            return s.run(cypher, Map.of("cap", 100_000))
                    .list(r -> r.get("id").isNull() ? null : r.get("id").asString())
                    .stream().filter(Objects::nonNull).toList();
        }
    }

    @Override
    public void close() {
        driver.close();
    }
}
