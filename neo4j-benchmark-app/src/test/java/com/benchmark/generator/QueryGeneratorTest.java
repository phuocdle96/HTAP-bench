package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.benchmark.generator.QueryGenerator.Engine.NEO4J;
import static org.junit.jupiter.api.Assertions.*;

class QueryGeneratorTest {

    static class FakeDb implements DatabaseClient {
        @Override public void connect() { }
        @Override public List<Map<String, Object>> executeQuery(String cypher, Map<String, Object> params) { return List.of(); }
        @Override public List<Map<String, Object>> executePrepared(QueryTemplate.PreparedQuery pq) { return List.of(); }
        @Override public List<String> fetchSampleIds(String label, String idProp) {
            // return a small but non-empty pool to avoid synthetic fallback
            if ("Patient".equals(label)) return List.of("P00001","P00002","P00003");
            if ("HealthcareUnit".equals(label)) return List.of("U001","U002");
            if ("Diagnosis".equals(label)) return List.of("D001","D002","D003");
            return List.of();
        }
        @Override public void close() { }
    }

    @Test
    void preparesPoolsWithParams() {
        DatabaseClient db = new FakeDb();
        QueryGenerator gen = new QueryGenerator(db, NEO4J);

        var pools = gen.prepareAllQueries();
        assertTrue(pools.containsKey("OLTP"));
        assertTrue(pools.containsKey("GRAPH"));
        assertTrue(pools.containsKey("OLAP"));

        var oltp = pools.get("OLTP");
        assertFalse(oltp.isEmpty());

        QueryTemplate.PreparedQuery first = oltp.get(0);
        assertNotNull(first.text);
        assertNotNull(first.params);
        assertTrue(first.params.containsKey("id"), "OLTP query should bind 'id'");
    }
}
