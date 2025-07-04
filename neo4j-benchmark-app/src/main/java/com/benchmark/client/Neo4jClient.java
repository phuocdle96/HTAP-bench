package com.benchmark.client;

import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Neo4j implementation of {@link DatabaseClient}.
 *
 * <p>One {@link Driver} per JVM (shared, thread-safe) and
 * one {@link Session} per *worker thread* via {@link ThreadLocal}.
 * Eliminates per-query handshake latency while avoiding the
 * “Existing open connection detected” error.</p>
 */
public class Neo4jClient implements DatabaseClient, AutoCloseable {

    private final Driver driver;
    private final ThreadLocal<Session> tlSession;
    private final String database;

    /* ---------- primary ctor ---------- */
    public Neo4jClient(String uri,
                       String user,
                       String password,
                       String database,
                       int timeoutSeconds) {

        this.database = database;

        AuthToken auth = AuthTokens.basic(user, password);

        Config cfg = Config.builder()
                .withConnectionTimeout(timeoutSeconds, SECONDS)
                .withConnectionAcquisitionTimeout(timeoutSeconds, SECONDS)
                .withMaxTransactionRetryTime(timeoutSeconds, SECONDS)
                .build();

        this.driver = GraphDatabase.driver(uri, auth, cfg);

        // each thread lazily creates *its own* Session
        this.tlSession = ThreadLocal.withInitial(
                () -> driver.session(SessionConfig.forDatabase(database)));
    }

    /* ---------- 4-arg back-compat ctor (60 s timeout) ---------- */
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
        return tlSession.get()
                        .run(cypher, params)
                        .list(org.neo4j.driver.Record::asMap);
    }

    @Override
    public List<String> fetchSampleIds(String labelOrQuery, String idProp) {
        String cypher =
            labelOrQuery.trim().toUpperCase().startsWith("MATCH")
            ? labelOrQuery
            : String.format("MATCH (n:%s) RETURN n.%s AS id LIMIT 10000",
                            labelOrQuery, idProp);

        return tlSession.get()
                        .run(cypher)
                        .list(r -> r.get("id").asString());
    }

    @Override
    public void close() {
        // close the session created by the current thread (BenchmarkRunner main)
        Session s = tlSession.get();
        if (s != null && s.isOpen()) s.close();
        driver.close();
    }
}
