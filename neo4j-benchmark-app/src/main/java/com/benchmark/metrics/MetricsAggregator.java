package com.benchmark.metrics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetricsAggregator {
    private final List<QueryResult> results;
    private final int durationSeconds;

    public MetricsAggregator(List<QueryResult> results, int durationSeconds) {
        this.results = results;
        this.durationSeconds = durationSeconds;
    }

    public void printReport() {
        System.out.println("\n--- Benchmark Results ---");
        if (results.isEmpty()) {
            System.out.println("No results recorded.");
            return;
        }

        Map<String, List<QueryResult>> resultsByCategory = results.stream()
                .collect(Collectors.groupingBy(QueryResult::category));

        double totalThroughput = (double) results.size() / durationSeconds;
        System.out.printf("Overall Throughput: %,.2f queries/sec%n", totalThroughput);
        System.out.printf("Total Queries Executed: %,d%n", results.size());
        System.out.println("----------------------------------------");

        resultsByCategory.entrySet().stream()
            .sorted(Map.Entry.comparingByKey()) // Sort categories for consistent order
            .forEach(entry -> {
                String category = entry.getKey();
                List<QueryResult> categoryResults = entry.getValue();
            
                DescriptiveStatistics stats = new DescriptiveStatistics();
                categoryResults.forEach(r -> stats.addValue(TimeUnit.NANOSECONDS.toMillis(r.latencyNanos())));

                double throughput = (double) categoryResults.size() / durationSeconds;

                System.out.printf("Category: %s%n", category);
                System.out.printf("  - Throughput:         %,.2f queries/sec%n", throughput);
                System.out.printf("  - Avg Latency:        %,.2f ms%n", stats.getMean());
                System.out.printf("  - Median Latency (p50): %,.2f ms%n", stats.getPercentile(50));
                System.out.printf("  - p95 Latency:        %,.2f ms%n", stats.getPercentile(95));
                System.out.printf("  - p99 Latency:        %,.2f ms%n", stats.getPercentile(99));
                System.out.printf("  - Min Latency:        %,.2f ms%n", stats.getMin());
                System.out.printf("  - Max Latency:        %,.2f ms%n", stats.getMax());
                System.out.println("----------------------------------------");
        });
    }
}