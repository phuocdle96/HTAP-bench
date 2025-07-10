package com.benchmark.metrics;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsAggregatorTest {

    @Test
    void testReportCalculations() throws Exception {   // ← add Exception to use Files.createTempFile
        // Create a list of sample results
        List<QueryResult> results = new ArrayList<>();
        // 100 OLTP queries, each took 10 ms
        for (int i = 0; i < 100; i++) {
            results.add(new QueryResult("OLTP", TimeUnit.MILLISECONDS.toNanos(10)));
        }
        // 20 OLAP queries, each took 100 ms
        for (int i = 0; i < 20; i++) {
            results.add(new QueryResult("OLAP", TimeUnit.MILLISECONDS.toNanos(100)));
        }

        int durationSeconds = 10;

        /* ---------- create temp CSV ---------- */
        Path tmpCsv = Files.createTempFile("aggTest", ".csv");

        Map<String,Integer> workerMap = Map.of("OLTP", 4, "OLAP", 1);
        MetricsAggregator agg = new MetricsAggregator(results, 10, tmpCsv.toString(), workerMap);

        agg.printReport();

        Files.deleteIfExists(tmpCsv);        // optional clean-up
    }
}
