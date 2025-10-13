package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.generator.QueryLanguage;

import java.util.List;
import java.util.Map;

public interface DatabaseClient extends AutoCloseable {

    void connect();

    /** Cypher execution pathway (Neo4j, Memgraph). */
    List<Map<String,Object>> executeQuery(String cypher, Map<String,Object> params);

    /** Back-compat default; JanusGraph overrides this for Gremlin. */
    default List<Map<String,Object>> executePrepared(QueryTemplate.PreparedQuery pq) {
        if (pq.lang == QueryLanguage.CYPHER) {
            return executeQuery(pq.text, pq.params);
        }
        throw new UnsupportedOperationException("Client does not support " + pq.lang);
    }

    /** Used by QueryGenerator to sample IDs for param generation. */
    List<String> fetchSampleIds(String label, String idProp);

    @Override
    void close();
}
