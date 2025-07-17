package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;

class ClientWorkerTest {

    private static QueryTemplate.PreparedQuery dummyPQ() {
        return new QueryTemplate.PreparedQuery("RETURN 1", Map.of());
    }

    @Test
    void singleShotWorkerRecordsOneResultWhenNotWarm() {
        /* ---- Mockito stub: every method returns defaults ---- */
        DatabaseClient dbMock = Mockito.mock(DatabaseClient.class);
        Mockito.when(dbMock.executeQuery(any(), anyMap()))
               .thenReturn(Collections.emptyList());

        BlockingQueue<QueryTemplate.PreparedQuery> qPool = new LinkedBlockingQueue<>();
        qPool.add(dummyPQ());

        List<QueryResult> collector = Collections.synchronizedList(new ArrayList<>());

        ClientWorker worker = new ClientWorker(
                dbMock,
                qPool,
                "OLTP",
                System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5),
                /* warm */ false,
                /* singleShot */ true,
                collector);

        worker.run();

        assertEquals(1, collector.size());
        assertEquals("OLTP", collector.get(0).category());
    }

    @Test
    void warmWorkerDoesNotRecordResults() {
        DatabaseClient dbMock = Mockito.mock(DatabaseClient.class);
        Mockito.when(dbMock.executeQuery(any(), anyMap()))
               .thenReturn(Collections.emptyList());

        BlockingQueue<QueryTemplate.PreparedQuery> qPool = new LinkedBlockingQueue<>();
        qPool.add(dummyPQ());

        List<QueryResult> collector = Collections.synchronizedList(new ArrayList<>());

        ClientWorker worker = new ClientWorker(
                dbMock,
                qPool,
                "GRAPH",
                System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5),
                /* warm */ true,
                /* singleShot */ true,
                collector);

        worker.run();

        assertEquals(0, collector.size());
    }
}
