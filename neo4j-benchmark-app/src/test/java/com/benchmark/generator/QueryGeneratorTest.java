package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryGeneratorTest {

    static class DummyDb implements DatabaseClient {
        @Override public void connect() {}
        @Override public List<Map<String, Object>> executeQuery(String q, Map<String, Object> p) { return List.of(); }
        @Override public List<Map<String, Object>> executePrepared(QueryTemplate.PreparedQuery pq) { return List.of(); }
        @Override public List<String> fetchSampleIds(String label, String idProp) {
            // deterministic samples for test
            return switch (label) {
                case "Patient"        -> List.of("P1","P2","P3");
                case "HealthcareUnit" -> List.of("H1","H2");
                case "Diagnosis"      -> List.of("D1","D2","D3","D4");
                default -> List.of("X");
            };
        }
        @Override public void close() {}
    }

    @Test
    void preparesPoolsForNeo4j() {
        QueryGenerator gen = new QueryGenerator(new DummyDb(), QueryGenerator.Engine.NEO4J);
        var pools = gen.prepareAllQueries();
        assertTrue(pools.get("OLTP").size()  > 0);
        assertTrue(pools.get("GRAPH").size() > 0);
        assertTrue(pools.get("OLAP").size()  > 0);
        // Neo4j/Memgraph => CYPHER
        assertEquals(QueryLanguage.CYPHER, pools.get("OLTP").get(0).lang);
    }

    @Test
    void preparesPoolsForJanusGraph() {
        QueryGenerator gen = new QueryGenerator(new DummyDb(), QueryGenerator.Engine.JANUSGRAPH);
        var pools = gen.prepareAllQueries();
        assertTrue(pools.get("OLTP").size()  > 0);
        assertTrue(pools.get("GRAPH").size() > 0);
        assertTrue(pools.get("OLAP").size()  > 0);
        // JanusGraph => GREMLIN
        assertEquals(QueryLanguage.GREMLIN, pools.get("OLTP").get(0).lang);
    }
}
