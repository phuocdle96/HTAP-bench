package com.benchmark.metrics;

import com.benchmark.util.ResultsLogger;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Computes summary statistics for a benchmark run and writes them to CSV.
 * QoL:
 *  - Shows Arrival Rate for OPEN mode (if provided).
 *  - Shows “Total (category)” for each category.
 */
public class MetricsAggregator {

    private final List<QueryResult>  snapshot;        // immutable copy
    private final List<FreshnessResult> freshness;    // kept for back-compat; not printed here
    private final int durationSeconds;
    private final ResultsLogger csv;
    private final Map<String,Integer> workers;

    // For OPEN mode UI (Arrival Rate instead of Workers)
    private final boolean openMode;
    private final Map<String, Double> arrivalRates;

    /* ---------------- Constructors ---------------- */

    // Original 4-arg (CLOSED mode back-compat)
    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             int durationSeconds,
                             String csvFile,
                             Map<String,Integer> workers) throws IOException {
        this(unsafeSharedList, null, durationSeconds, csvFile, workers, false, Map.of());
    }

    // Original 5-arg with freshness list (back-compat)
    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             List<FreshnessResult> freshnessResults,
                             int durationSeconds,
                             String csvFile,
                             Map<String,Integer> workers) throws IOException {
        this(unsafeSharedList, freshnessResults, durationSeconds, csvFile, workers, false, Map.of());
    }

    // NEW 6-arg bridge (what your BenchmarkRunner is calling)
    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             int durationSeconds,
                             String csvFile,
                             Map<String,Integer> workers,
                             boolean openMode,
                             Map<String, Double> arrivalRates) throws IOException {
        this(unsafeSharedList, null, durationSeconds, csvFile, workers, openMode, arrivalRates);
    }

    // NEW 7-arg canonical constructor (OPEN/CLOSED unified)
    public MetricsAggregator(List<QueryResult> unsafeSharedList,
                             List<FreshnessResult> freshnessResults,
                             int durationSeconds,
                             String csvFile,
                             Map<String,Integer> workers,
                             boolean openMode,
                             Map<String, Double> arrivalRates) throws IOException {
        synchronized (unsafeSharedList) {
            this.snapshot = List.copyOf(unsafeSharedList);
        }
        this.freshness = (freshnessResults == null) ? List.of() : List.copyOf(freshnessResults);
        this.durationSeconds = durationSeconds;
        this.workers = (workers == null) ? Map.of() : workers;
        this.openMode = openMode;
        this.arrivalRates = (arrivalRates == null) ? Map.of() : arrivalRates;

        this.csv = new ResultsLogger(csvFile,
                "timestamp","category","workers",
                "total_queries","throughput_ops",
                "avg_latency_ms","median_latency_ms",
                "p95_latency_ms","p99_latency_ms",
                "min_latency_ms","max_latency_ms");
    }

    /* ---------------- Public ---------------- */

    public void printReport() {
        System.out.println("\n--- Benchmark Results ---");

        if (snapshot.isEmpty()) {
            System.out.println("No results recorded (all threads failed or aborted).");
        } else {
            Map<String, List<QueryResult>> byCategory = snapshot.stream()
                    .collect(Collectors.groupingBy(QueryResult::category));

            double overallQps = (double) snapshot.size() / Math.max(1, durationSeconds);
            System.out.printf("Overall Throughput: %,.2f queries/sec%n", overallQps);
            System.out.printf("Total Queries Executed: %,d%n", snapshot.size());
            System.out.println("----------------------------------------");

            for (String cat : List.of("OLAP","OLTP","GRAPH")) {
                List<QueryResult> list = byCategory.get(cat);
                if (list != null && !list.isEmpty()) {
                    printOneCategory(cat, list);
                }
            }

            // overall row to CSV
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
        // Freshness printing is handled by HeartbeatService; this class focuses on throughput/latency.
    }

    /* ---------------- Internals ---------------- */

    private void printOneCategory(String cat, List<QueryResult> list) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        list.forEach(r -> stats.addValue(
                TimeUnit.NANOSECONDS.toMillis(r.latencyNanos())));

        double qps = (double) list.size() / Math.max(1, durationSeconds);
        long total = list.size();

        System.out.printf("Category: %s%n", cat);
        if (openMode) {
            double rate = arrivalRates.getOrDefault(cat, 0.0);
            System.out.printf("  - Arrival Rate:         %s qps%n", fmtRate(rate));
        } else {
            System.out.printf("  - Workers:             %d%n", workers.getOrDefault(cat, 0));
        }
        System.out.printf("  - Throughput:          %,.2f queries/sec%n", qps);
        System.out.printf("  - Total (category):     %,d%n", total);
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
                    "total_queries",   total,
                    "throughput_ops",  String.format(Locale.US, "%.2f", qps),
                    "avg_latency_ms",  String.format(Locale.US, "%.2f", stats.getMean()),
                    "median_latency_ms", String.format(Locale.US, "%.2f", stats.getPercentile(50)),
                    "p95_latency_ms",  String.format(Locale.US, "%.2f", stats.getPercentile(95)),
                    "p99_latency_ms",  String.format(Locale.US, "%.2f", stats.getPercentile(99)),
                    "min_latency_ms",  String.format(Locale.US, "%.2f", stats.getMin()),
                    "max_latency_ms",  String.format(Locale.US, "%.2f", stats.getMax())
            ));
        } catch (IOException e) {
            System.err.println("CSV write failed: " + e.getMessage());
        }
    }

    private static String fmtRate(Double d) {
        if (d == null) return "0.0";
        if (d < 1.0) return String.format(Locale.US, "%.2f", d);
        return String.format(Locale.US, "%.0f", d);
    }
}

