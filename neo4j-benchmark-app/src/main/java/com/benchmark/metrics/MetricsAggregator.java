package com.benchmark.metrics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetricsAggregator {

    private final List<QueryResult> snapshot;     // immutable copy
    private final int durationSeconds;

    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             int durationSeconds) {
        // defensive copy to avoid ConcurrentModificationException
        synchronized (unsafeSharedList) {
            this.snapshot = List.copyOf(unsafeSharedList);
        }
        this.durationSeconds = durationSeconds;
    }

    public void printReport() {
        System.out.println("\n--- Benchmark Results ---");

        if (snapshot.isEmpty()) {
            System.out.println("No results recorded (all threads failed or aborted).");
            return;
        }

        Map<String, List<QueryResult>> byCategory = snapshot.stream()
                .collect(Collectors.groupingBy(QueryResult::category));

        double overallQps = (double) snapshot.size() / durationSeconds;
        System.out.printf("Overall Throughput: %,.2f queries/sec%n", overallQps);
        System.out.printf("Total Queries Executed: %,d%n", snapshot.size());
        System.out.println("----------------------------------------");

        byCategory.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> printOneCategory(e.getKey(), e.getValue()));
    }

    /* helper */
    private void printOneCategory(String cat, List<QueryResult> list) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        list.forEach(r -> stats.addValue(
                TimeUnit.NANOSECONDS.toMillis(r.latencyNanos())));

        double qps = (double) list.size() / durationSeconds;

        System.out.printf("Category: %s%n", cat);
        System.out.printf("  - Throughput:         %,.2f queries/sec%n", qps);
        System.out.printf("  - Avg Latency:        %,.2f ms%n", stats.getMean());
        System.out.printf("  - Median Latency (p50): %,.2f ms%n", stats.getPercentile(50));
        System.out.printf("  - p95 Latency:        %,.2f ms%n", stats.getPercentile(95));
        System.out.printf("  - p99 Latency:        %,.2f ms%n", stats.getPercentile(99));
        System.out.printf("  - Min Latency:        %,.2f ms%n", stats.getMin());
        System.out.printf("  - Max Latency:        %,.2f ms%n", stats.getMax());
        System.out.println("----------------------------------------");
    }
}
