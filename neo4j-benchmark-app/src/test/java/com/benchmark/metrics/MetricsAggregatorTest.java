package com.benchmark.metrics;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsAggregatorTest {

    @Test
    void testReportCalculations() {
        // Create a list of sample results
        List<QueryResult> results = new ArrayList<>();
        // 100 OLTP queries, each took 10ms (10,000,000 ns)
        for (int i = 0; i < 100; i++) {
            results.add(new QueryResult("OLTP", TimeUnit.MILLISECONDS.toNanos(10)));
        }
        // 20 OLAP queries, each took 100ms (100,000,000 ns)
        for (int i = 0; i < 20; i++) {
            results.add(new QueryResult("OLAP", TimeUnit.MILLISECONDS.toNanos(100)));
        }

        // Run the aggregator over a hypothetical 10-second duration
        int durationSeconds = 10;
        
        // This is a simple test to ensure no exceptions are thrown.
        // A more robust test would capture System.out and assert its contents.
        MetricsAggregator aggregator = new MetricsAggregator(results, durationSeconds);
        aggregator.printReport(); 
        
        // Example of a more specific assertion you could make
        double expectedOltpThroughput = 100.0 / 10.0;
        // You would need to capture the output to properly test this value.
        // assertEquals(expectedOltpThroughput, capturedThroughput);
    }
}