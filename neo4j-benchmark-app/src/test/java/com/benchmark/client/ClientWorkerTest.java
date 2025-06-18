package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClientWorkerTest {

    @Mock
    private DatabaseClient mockDbClient;

    private Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> queryQueues;

    @BeforeEach
    void setUp() {
        queryQueues = new HashMap<>();
        BlockingQueue<QueryTemplate.PreparedQuery> oltpQueue = new LinkedBlockingQueue<>();
        oltpQueue.add(new QueryTemplate.PreparedQuery("MATCH (n) RETURN n", Collections.emptyMap()));
        queryQueues.put("OLTP", oltpQueue);
    }

    @Test
    void testWorkerExecutesQueryAndRecordsResult() throws Exception {
        long endTime = System.currentTimeMillis() + 100;
        ClientWorker worker = new ClientWorker(mockDbClient, queryQueues, endTime, false);

        List<QueryResult> results = worker.call();

        // CORRECTED: Verify the method was called at least once
        verify(mockDbClient, atLeastOnce()).executeQuery(anyString(), any(Map.class));
        assertFalse(results.isEmpty());
        assertEquals("OLTP", results.get(0).category());
        assertTrue(results.get(0).latencyNanos() > 0);
    }

    @Test
    void testWarmupRunDoesNotRecordResults() throws Exception {
        long endTime = System.currentTimeMillis() + 100;
        ClientWorker worker = new ClientWorker(mockDbClient, queryQueues, endTime, true);

        List<QueryResult> results = worker.call();

        // CORRECTED: Verify the method was called at least once
        verify(mockDbClient, atLeastOnce()).executeQuery(anyString(), any(Map.class));
        assertTrue(results.isEmpty());
    }
}