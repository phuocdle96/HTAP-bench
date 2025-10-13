package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryGeneratorTest {

    /** Minimal fake DB that returns deterministic sample IDs and echoes queries. */
    private static class FakeDb implements DatabaseClient {
        @Override public void connect() { /* no-op */ }

        @Override
        public List<Map<String, Object>> executeQuery(String text, Map<String, Object> params) {
            // pretend the DB returned one row with whatever we sent
            return List.of(Map.of("text", text, "params", params == null ? Map.of() : params));
        }

        @Override
        public List<String> fetchSampleIds(String label, String idProperty) {
            // provide a small fixed set of IDs so parameterized templates can materialize
            return List.of("1", "2", "3", "4", "5");
        }

        @Override public void close() { /* no-op */ }
    }

    @Test
    void prepareAllQueries_producesNonEmptyPools() {
        DatabaseClient db = new FakeDb();

        // NOTE: QueryGenerator now requires an Engine argument
        QueryGenerator gen = new QueryGenerator(db, QueryGenerator.Engine.NEO4J);

        Map<String, List<QueryTemplate.PreparedQuery>> pools = gen.prepareAllQueries();

        assertNotNull(pools, "pools map must not be null");
        assertTrue(pools.containsKey("OLTP"),  "missing OLTP pool");
        assertTrue(pools.containsKey("GRAPH"), "missing GRAPH pool");
        assertTrue(pools.containsKey("OLAP"),  "missing OLAP pool");

        // each category has at least one prepared query
        assertFalse(pools.get("OLTP").isEmpty(),  "OLTP pool is empty");
        assertFalse(pools.get("GRAPH").isEmpty(), "GRAPH pool is empty");
        assertFalse(pools.get("OLAP").isEmpty(),  "OLAP pool is empty");

        // spot-check instances are non-null
        assertNotNull(pools.get("OLTP").getFirst(),  "first OLTP prepared query is null");
        assertNotNull(pools.get("GRAPH").getFirst(), "first GRAPH prepared query is null");
        assertNotNull(pools.get("OLAP").getFirst(),  "first OLAP prepared query is null");
    }
}
