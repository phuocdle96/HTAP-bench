package com.benchmark.client;

import com.benchmark.generator.QueryLanguage;
import com.benchmark.generator.QueryTemplate;

import java.util.List;
import java.util.Map;

public interface DatabaseClient extends AutoCloseable {

    void connect();

    /** Cypher execution pathway (Neo4j, Memgraph). */
    List<Map<String,Object>> executeQuery(String cypher, Map<String,Object> params);

    /**
     * OPTIONAL override: honor readOnly (OLAP/GRAPH) vs write (OLTP).
     * Default ignores readOnly and calls the 2-arg variant.
     */
    default List<Map<String,Object>> executeQuery(String cypher, Map<String,Object> params, boolean readOnly) {
        return executeQuery(cypher, params);
    }

    /** Back-compat default; JanusGraph overrides this for Gremlin. */
    default List<Map<String,Object>> executePrepared(QueryTemplate.PreparedQuery pq) {
        if (pq.lang == QueryLanguage.CYPHER) {
            return executeQuery(pq.text, pq.params);
        }
        throw new UnsupportedOperationException("Client does not support " + pq.lang);
    }

    /**
     * Prepared execution with readOnly hint (used by ClientWorker).
     * - CYPHER: route to executeQuery(..., readOnly).
     * - GREMLIN: fall back to executePrepared (readOnly not applicable).
     */
    default List<Map<String,Object>> executePreparedWithMode(QueryTemplate.PreparedQuery pq, boolean readOnly) {
        if (pq.lang == QueryLanguage.CYPHER) {
            return executeQuery(pq.text, pq.params, readOnly);
        }
        return executePrepared(pq);
    }

    /** Used by QueryGenerator to sample IDs for param generation. */
    List<String> fetchSampleIds(String label, String idProp);

    @Override
    void close();
}
