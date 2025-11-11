package com.benchmark.client;

import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;

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
                .withMaxTransactionRetryTime(timeoutSeconds, SECONDS)  // retry transient conflicts
                .withMaxConnectionPoolSize(800)
                .build();

        this.driver = GraphDatabase.driver(uri, auth, cfg);
    }

    public Neo4jClient(String uri, String user, String password, String database) {
        this(uri, user, password, database, 60);
    }

    @Override public void connect() { /* driver created in ctor */ }

    @Override
    public List<Map<String,Object>> executeQuery(String cypher, Map<String,Object> params) {
        return executeQuery(cypher, params, false);
    }

    @Override
    public List<Map<String, Object>> executeQuery(String cypher, Map<String, Object> params, boolean readOnly) {
        try (Session s = driver.session(SessionConfig.builder()
                .withDatabase(database)
                .withDefaultAccessMode(readOnly ? AccessMode.READ : AccessMode.WRITE)
                .build())) {

            if (readOnly) {
                return s.executeRead(tx -> runTx(tx, cypher, params));
            } else {
                return s.executeWrite(tx -> runTx(tx, cypher, params));
            }
        }
    }

    private static List<Map<String,Object>> runTx(TransactionContext tx, String cypher, Map<String,Object> params) {
        Result res = (params == null || params.isEmpty()) ? tx.run(cypher) : tx.run(cypher, params);
        return res.list(org.neo4j.driver.Record::asMap);
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
            return s.executeRead(tx -> tx.run(cypher, Map.of("cap", 10_000))
                    .list(r -> r.get("id").isNull() ? null : r.get("id").asString()))
                    .stream().filter(java.util.Objects::nonNull).toList();
        }
    }

    @Override
    public void close() {
        driver.close();
    }
}

