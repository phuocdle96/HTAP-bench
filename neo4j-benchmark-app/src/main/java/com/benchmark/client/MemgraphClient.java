package com.benchmark.client;

import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Memgraph implementation backed by Neo4j Bolt driver.
 *
 * - Writer URI: --uri
 * - Read replicas (optional): --memgraph-read-uris=bolt://r1:7687,bolt://r2:7687
 * - OLAP/GRAPH queries are routed to READ replicas when available.
 */
public class MemgraphClient implements DatabaseClient, AutoCloseable {

    private final String writerUri;
    private final String user;
    private final String password;
    private final List<String> readUris;

    private Driver writer;
    private List<Driver> readers;
    private final AtomicInteger rr = new AtomicInteger(0);

    public MemgraphClient(String uri, String user, String password) {
        this(uri, user, password, List.of());
    }

    public MemgraphClient(String writerUri, String user, String password, List<String> readUris) {
        this.writerUri = writerUri;
        this.user = (user == null ? "" : user);
        this.password = (password == null ? "" : password);
        this.readUris = (readUris == null) ? List.of()
                : readUris.stream().filter(s -> s != null && !s.isBlank()).collect(Collectors.toList());
    }

    @Override
    public void connect() {
        AuthToken auth = AuthTokens.basic(user, password);
        Config cfg = Config.builder()
                .withConnectionTimeout(30, SECONDS)
                .withConnectionAcquisitionTimeout(30, SECONDS)
                .withMaxTransactionRetryTime(30, SECONDS)   // driver-level retry for TransientException
                .withMaxConnectionPoolSize(800)
                .build();

        this.writer = GraphDatabase.driver(writerUri, auth, cfg);
        this.readers = readUris.stream()
                .map(uri -> GraphDatabase.driver(uri, auth, cfg))
                .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> executeQuery(String cypher, Map<String, Object> params) {
        return executeQuery(cypher, params, false);
    }

    @Override
    public List<Map<String, Object>> executeQuery(String cypher, Map<String, Object> params, boolean readOnly) {
        Driver d = pickDriver(readOnly);

        try (Session s = d.session(SessionConfig.builder()
                .withDefaultAccessMode(readOnly ? AccessMode.READ : AccessMode.WRITE)
                .build())) {

            if (readOnly) {
                return s.executeRead(tx -> runTx(tx, cypher, params));
            } else {
                return s.executeWrite(tx -> runTx(tx, cypher, params)); // retryable on conflicts
            }
        }
    }

    private static List<Map<String,Object>> runTx(TransactionContext tx, String cypher, Map<String,Object> params) {
        Result res = (params == null || params.isEmpty()) ? tx.run(cypher) : tx.run(cypher, params);
        return res.list(org.neo4j.driver.Record::asMap);
    }

    private Driver pickDriver(boolean readOnly) {
        if (readOnly && readers != null && !readers.isEmpty()) {
            int i = Math.floorMod(rr.getAndIncrement(), readers.size());
            return readers.get(i);
        }
        return writer;
    }

    @Override
    public List<String> fetchSampleIds(String label, String idProperty) {
        String cypher = "MATCH (n:`" + label + "`) " +
                        "WHERE n.`" + idProperty + "` IS NOT NULL " +
                        "RETURN n.`" + idProperty + "` AS id LIMIT $cap";
        Driver d = pickDriver(true);
        try (Session s = d.session(SessionConfig.builder()
                .withDefaultAccessMode(AccessMode.READ)
                .build())) {
            return s.executeRead(tx -> tx.run(cypher, Map.of("cap", 100_000))
                    .list(r -> r.get("id").isNull() ? null : r.get("id").asString()))
                    .stream().filter(Objects::nonNull).toList();
        }
    }

    @Override
    public void close() {
        if (writer != null) writer.close();
        if (readers != null) readers.forEach(Driver::close);
    }
}

