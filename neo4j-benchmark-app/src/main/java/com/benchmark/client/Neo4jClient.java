package com.benchmark.client;

import org.neo4j.driver.*;                    // Driver, AuthTokens, Config, Session
import java.util.List;
import java.util.Map;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Neo4j implementation of {@link DatabaseClient}.
 * Driver time-outs follow the benchmark duration (timeoutSeconds).
 */
public class Neo4jClient implements DatabaseClient, AutoCloseable {

    private final Driver driver;
    private final String database;

    /* ---------- primary ctor ---------- */
    public Neo4jClient(String uri,
                       String user,
                       String password,
                       String database,
                       int timeoutSeconds) {

        AuthToken auth = AuthTokens.basic(user, password);

        Config cfg = Config.builder()
                .withConnectionTimeout(timeoutSeconds, SECONDS)
                .withConnectionAcquisitionTimeout(timeoutSeconds, SECONDS)
                .withMaxTransactionRetryTime(timeoutSeconds, SECONDS)
                .build();

        this.driver   = GraphDatabase.driver(uri, auth, cfg);
        this.database = database;
    }

    /* ---------- 4-arg back-compat ctor (60 s) ---------- */
    public Neo4jClient(String uri,
                       String user,
                       String password,
                       String database) {
        this(uri, user, password, database, 60);
    }

    /* ---------- DatabaseClient ---------- */
    @Override public void connect() { /* driver opened in ctor */ }

    @Override
    public List<Map<String,Object>> executeQuery(String cypher,
                                                 Map<String,Object> params) {
        try (Session ses = driver.session(SessionConfig.forDatabase(database))) {
            return ses.run(cypher, params)
                      .list(org.neo4j.driver.Record::asMap);          // <-- fully-qualified
        }
    }

    @Override
    public List<String> fetchSampleIds(String labelOrQuery, String idProp) {
        String cypher =
            labelOrQuery.trim().toUpperCase().startsWith("MATCH")
            ? labelOrQuery
            : String.format("MATCH (n:%s) RETURN n.%s AS id LIMIT 10000",
                            labelOrQuery, idProp);

        try (Session ses = driver.session(SessionConfig.forDatabase(database))) {
            return ses.run(cypher)
                      .list(r -> r.get("id").asString());             // r is org.neo4j.driver.Record
        }
    }

    @Override public void close() { driver.close(); }
}
