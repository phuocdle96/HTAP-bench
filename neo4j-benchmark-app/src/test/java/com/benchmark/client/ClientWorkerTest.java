package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class ClientWorkerTest {

    /** Dummy DB client that returns instantly. */
    private static class NoopDb implements DatabaseClient {
        @Override public void connect()            {}
        @Override public List<String> fetchSampleIds(String l, String p){ return List.of(); }
        @Override public List<java.util.Map<String,Object>> executeQuery(String c, java.util.Map<String,Object> m){ return List.of(); }
        @Override public void close()              {}
    }

    private static QueryTemplate.PreparedQuery dummyPQ() {
        return new QueryTemplate.PreparedQuery("RETURN 1", java.util.Map.of());
    }

    @Test
    void testClosedLoopCollectsLatency() throws Exception {
        BlockingQueue<QueryTemplate.PreparedQuery> q = new ArrayBlockingQueue<>(1);
        q.add(dummyPQ());

        ClientWorker w = new ClientWorker(
                new NoopDb(),
                q,
                /* tokenQueue = */ null,      // CLOSED mode
                "OLTP",
                System.currentTimeMillis() + 1_000,   // run ≤1 s
                false);

        List<QueryResult> r = w.call();
        assertFalse(r.isEmpty(), "Should record at least one result");
        assertEquals("OLTP", r.get(0).category());
    }

    @Test
    void testWarmupIgnored() throws Exception {
        BlockingQueue<QueryTemplate.PreparedQuery> q = new ArrayBlockingQueue<>(1);
        q.add(dummyPQ());

        ClientWorker w = new ClientWorker(
                new NoopDb(),
                q,
                /* tokenQueue = */ null,      // CLOSED mode
                "OLTP",
                System.currentTimeMillis() + 200,  // very short
                true);                           // warm-up

        List<QueryResult> r = w.call();
        assertTrue(r.isEmpty(), "Warm-up run should not record results");
    }
}
