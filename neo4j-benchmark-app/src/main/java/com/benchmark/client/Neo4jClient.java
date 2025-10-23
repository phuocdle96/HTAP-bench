package com.benchmark.client;

import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Neo4j implementation of {@link DatabaseClient}.
 *
 * - One Driver per JVM (thread-safe).
 * - Open a Session per query via try-with-resources (vthread-friendly).
 */
public class Neo4jClient implements DatabaseClient, AutoCloseable {

    private final Driver driver;
    private final String database;

    public Neo4jClient(String uri, String user, String password, String database, int timeoutSeconds) {
        this.database = database;

        AuthToken auth = AuthTokens.basic(user, password);
        Config cfg = Config.builder()
                .withConnectionTimeout(timeoutSeconds, SECONDS)
                .withConnectionAcquisitionTimeout(timeoutSeconds, SECONDS)
                .withMaxTransactionRetryTime(timeoutSeconds, SECONDS)
                .withMaxConnectionPoolSize(400)
                .build();

        this.driver = GraphDatabase.driver(uri, auth, cfg);
    }

    public Neo4jClient(String uri, String user, String password, String database) {
        this(uri, user, password, database, 60);
    }

    @Override public void connect() { /* driver created in ctor */ }

    @Override
    public List<Map<String,Object>> executeQuery(String cypher, Map<String,Object> params) {
        try (Session s = driver.session(SessionConfig.forDatabase(database))) {
            return (params == null || params.isEmpty())
                    ? s.run(cypher).list(org.neo4j.driver.Record::asMap)
                    : s.run(cypher, params).list(org.neo4j.driver.Record::asMap);
        }
    }

    @Override
    public List<String> fetchSampleIds(String labelOrQuery, String idProp) {
        final String cypher =
                labelOrQuery.trim().toUpperCase().startsWith("MATCH")
                        ? labelOrQuery
                        : "MATCH (n:`" + labelOrQuery + "`) " +
                          "WHERE n.`" + idProp + "` IS NOT NULL " +
                          "RETURN n.`" + idProp + "` AS id LIMIT $cap";

        try (Session s = driver.session(SessionConfig.forDatabase(database))) {
            return s.run(cypher, Map.of("cap", 10_000))
                    .list(r -> r.get("id").isNull() ? null : r.get("id").asString())
                    .stream().filter(Objects::nonNull).toList();
        }
    }

    @Override
    public void close() {
        driver.close();
    }
}
