package com.benchmark.metrics;

import com.benchmark.util.ResultsLogger;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Computes summary statistics for a benchmark run and writes them to CSV.
 * Now also supports an optional Freshness section (HB lag).
 */
public class MetricsAggregator {

    private final List<QueryResult>  snapshot;        // immutable copy
    private final List<FreshnessResult> freshness;    // may be null or empty
    private final int durationSeconds;
    private final ResultsLogger csv;
    private final Map<String,Integer> workers;

    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             int durationSeconds,
                             String csvFile,
                             Map<String,Integer> workers) throws IOException {
        this(unsafeSharedList, null, durationSeconds, csvFile, workers);
    }

    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             List<FreshnessResult> freshnessResults,
                             int durationSeconds,
                             String csvFile,
                             Map<String,Integer> workers) throws IOException {
        synchronized (unsafeSharedList) {
            this.snapshot = List.copyOf(unsafeSharedList);
        }
        this.freshness = (freshnessResults == null) ? List.of() : List.copyOf(freshnessResults);
        this.durationSeconds = durationSeconds;
        this.workers = workers;
        this.csv = new ResultsLogger(csvFile,
                "timestamp","category","workers",
                "total_queries","throughput_ops",
                "avg_latency_ms","median_latency_ms",
                "p95_latency_ms","p99_latency_ms",
                "min_latency_ms","max_latency_ms");
    }

    public void printReport() {
        System.out.println("\n--- Benchmark Results ---");

        if (snapshot.isEmpty()) {
            System.out.println("No results recorded (all threads failed or aborted).");
        } else {
            Map<String, List<QueryResult>> byCategory = snapshot.stream()
                    .collect(Collectors.groupingBy(QueryResult::category));

            double overallQps = (double) snapshot.size() / durationSeconds;
            System.out.printf("Overall Throughput: %,.2f queries/sec%n", overallQps);
            System.out.printf("Total Queries Executed: %,d%n", snapshot.size());
            System.out.println("----------------------------------------");

            byCategory.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> printOneCategory(e.getKey(), e.getValue()));

            // overall row
            try {
                csv.log(Map.of(
                        "category", "OVERALL",
                        "workers",  workers.values().stream().mapToInt(Integer::intValue).sum(),
                        "total_queries", snapshot.size(),
                        "throughput_ops", String.format(Locale.US, "%.2f", overallQps),
                        "avg_latency_ms", "",
                        "median_latency_ms", "",
                        "p95_latency_ms", "",
                        "p99_latency_ms", "",
                        "min_latency_ms", "",
                        "max_latency_ms", ""
                ));
            } catch (IOException e) {
                System.err.println("CSV write failed: " + e.getMessage());
            }

            try { csv.close(); } catch (IOException ignore) {}
        }

        // Freshness section (if any)
        if (!freshness.isEmpty()) {
            printFreshnessSection(freshness);
        }
    }

    private void printOneCategory(String cat, List<QueryResult> list) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        list.forEach(r -> stats.addValue(
                TimeUnit.NANOSECONDS.toMillis(r.latencyNanos())));

        double qps = (double) list.size() / durationSeconds;

        System.out.printf("Category: %s%n", cat);
        System.out.printf("  - Workers:             %d%n", workers.getOrDefault(cat, 0));
        System.out.printf("  - Throughput:          %,.2f queries/sec%n", qps);
        System.out.printf("  - Avg Latency:          %,.2f ms%n", stats.getMean());
        System.out.printf("  - Median Latency (p50): %,.2f ms%n", stats.getPercentile(50));
        System.out.printf("  - p95 Latency:          %,.2f ms%n", stats.getPercentile(95));
        System.out.printf("  - p99 Latency:          %,.2f ms%n", stats.getPercentile(99));
        System.out.printf("  - Min Latency:          %,.2f ms%n", stats.getMin());
        System.out.printf("  - Max Latency:          %,.2f ms%n", stats.getMax());
        System.out.println("----------------------------------------");

        try {
            csv.log(Map.of(
                    "category", cat,
                    "workers",         workers.getOrDefault(cat, 0),
                    "total_queries", list.size(),
                    "throughput_ops", String.format(Locale.US, "%.2f", qps),
                    "avg_latency_ms", String.format(Locale.US, "%.2f", stats.getMean()),
                    "median_latency_ms", String.format(Locale.US, "%.2f", stats.getPercentile(50)),
                    "p95_latency_ms", String.format(Locale.US, "%.2f", stats.getPercentile(95)),
                    "p99_latency_ms", String.format(Locale.US, "%.2f", stats.getPercentile(99)),
                    "min_latency_ms", String.format(Locale.US, "%.2f", stats.getMin()),
                    "max_latency_ms", String.format(Locale.US, "%.2f", stats.getMax())
            ));
        } catch (IOException e) {
            System.err.println("CSV write failed: " + e.getMessage());
        }
    }

    private void printFreshnessSection(List<FreshnessResult> fr) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        fr.forEach(x -> stats.addValue(x.freshnessMillis()));

        System.out.println("\n--- Freshness (OLAP visibility lag) ---");
        System.out.printf("Samples: %,d%n", fr.size());
        System.out.printf("  - Avg Freshness:          %,.2f ms%n", stats.getMean());
        System.out.printf("  - Median Freshness (p50): %,.2f ms%n", stats.getPercentile(50));
        System.out.printf("  - p95 Freshness:          %,.2f ms%n", stats.getPercentile(95));
        System.out.printf("  - p99 Freshness:          %,.2f ms%n", stats.getPercentile(99));
        System.out.printf("  - Min Freshness:          %,.2f ms%n", stats.getMin());
        System.out.printf("  - Max Freshness:          %,.2f ms%n", stats.getMax());
        System.out.println("----------------------------------------");

        // also write to a separate CSV
        try (ResultsLogger fcsv = new ResultsLogger("freshness_results.csv",
                "timestamp","freshness_ms")) {
            for (FreshnessResult x : fr) {
                fcsv.log(Map.of("freshness_ms", x.freshnessMillis()));
            }
        } catch (IOException e) {
            System.err.println("Freshness CSV write failed: " + e.getMessage());
        }
    }
}
