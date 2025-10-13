// neo4j-benchmark-app/src/main/java/com/benchmark/BenchmarkRunner.java
// ------------------------------------------------------------------
// Updated 2025-10-13
//  • OPEN/CLOSED modes with virtual threads
//  • OPEN ramping: per-interval arrival-rate increases and per-interval reports
//  • Interval-scoped results so interval throughput/latency are correct
//  • Final overall report aggregates all intervals
//  • Safe bounded worker pool; sharded submitters with per-shard QPS cap
//  • Warmup runs in CLOSED mode
//  • Catches IOException from MetricsAggregator.printReport()
//  • NEW: compute single-interval duration once, CSV per interval, queue pressure logs
//
package com.benchmark;

import com.benchmark.arrival.ArrivalMode;
import com.benchmark.arrival.ArrivalSubmitter;
import com.benchmark.client.ClientWorker;
import com.benchmark.client.DatabaseClient;
import com.benchmark.client.Neo4jClient;
import com.benchmark.generator.QueryGenerator;
import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.MetricsAggregator;
import com.benchmark.metrics.QueryResult;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

@Command(
    name = "benchmark-runner",
    mixinStandardHelpOptions = true,
    description = "HTAP benchmark – CLOSED (token) and OPEN (Poisson) arrival modes with optional ramping"
)
public class BenchmarkRunner implements Callable<Integer> {

    /* ---------------- CLI flags ---------------- */
    @Option(names = "--uri",      defaultValue = "bolt://localhost:7687") String uri;
    @Option(names = "--user",     defaultValue = "neo4j")                 String user;
    @Option(names = "--password", defaultValue = "neo4j")                 String password;
    @Option(names = "--database", defaultValue = "neo4j")                 String database;

    // CLOSED-loop worker counts
    @Option(names = "--oltp-clients",  defaultValue = "8") int oltpClients;
    @Option(names = "--graph-clients", defaultValue = "1") int graphClients;
    @Option(names = "--olap-clients",  defaultValue = "1") int olapClients;

    // OPEN-loop arrival rates (QPS)
    @Option(names = "--arrival-mode", defaultValue = "CLOSED") ArrivalMode arrivalMode;
    @Option(names = "--arrival-rate", arity = "1..*", split = ",",
            description = "Per-category λ, e.g. OLTP=2000,GRAPH=50,OLAP=0.1 (OPEN mode only)")
    List<String> arrivalPairs = new ArrayList<>();

    // Duration
    @Option(names = {"-d", "--duration"}, defaultValue = "180") int durationSeconds;
    @Option(names = {"-w", "--warmup"},   defaultValue = "10")  int warmupSeconds;

    // Ramping (OPEN mode): interval seconds and per-interval λ additions
    @Option(names="--ramp-interval", defaultValue="0",
            description = "Seconds between ramps in OPEN mode (0 = no ramping)")
    int rampIntervalSec;

    // Example: --ramp-rate-step OLTP=250,GRAPH=5,OLAP=0.02
    @Option(names="--ramp-rate-step", arity="0..*", split=",",
            description = "Per-interval λ increment per category (OPEN mode)")
    List<String> rampRatePairs = new ArrayList<>();

    // Optional max λ clamp: --max-arrival-rate OLTP=4000,GRAPH=100
    @Option(names="--max-arrival-rate", arity="0..*", split=",",
            description = "Max λ per category (OPEN mode)")
    List<String> maxRatePairs = new ArrayList<>();

    @Option(names="--engine", defaultValue = "NEO4J",
            description = "Query engine: NEO4J | MEMGRAPH | JANUSGRAPH")
    String engineName;

    /* ---------------- constants ---------------- */
    private static final int MAX_V_THREADS     = 512;      // bounded worker concurrency
    private static final int MAX_BACKLOG       = 20_000;   // queue size
    private static final int MAX_SUBMITTER_QPS = 300;      // per shard QPS cap

    /* ---------------- internal ---------------- */
    private final Map<String, Double> λ = new HashMap<>();                           // current arrival rates
    private final List<QueryResult>    results = Collections.synchronizedList(new ArrayList<>()); // global for final summary

    /* =============================== helper parsers =============================== */
    private static Map<String,Double> parseDoubleMap(List<String> pairs) {
        Map<String,Double> m = new HashMap<>();
        for (String s : pairs) {
            if (s == null || s.isBlank()) continue;
            String[] a = s.split("=", 2);
            if (a.length == 2) m.put(a[0].trim().toUpperCase(Locale.ROOT), Double.parseDouble(a[1].trim()));
        }
        return m;
    }
    private static Map<String,Integer> parseIntMap(List<String> pairs) {
        Map<String,Integer> m = new HashMap<>();
        for (String s : pairs) {
            if (s == null || s.isBlank()) continue;
            String[] a = s.split("=", 2);
            if (a.length == 2) m.put(a[0].trim().toUpperCase(Locale.ROOT), Integer.parseInt(a[1].trim()));
        }
        return m;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    /* ========================================================================== */
    @Override
    public Integer call() throws Exception {
        Instant startTs = Instant.now();
        System.out.printf("Benchmark started at %s%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(startTs.atZone(ZoneId.systemDefault()).toLocalTime()));

        /* ---------- parse OPEN λ from CLI ---------- */
        arrivalPairs.stream()
                .map(s -> s.split("="))
                .filter(a -> a.length == 2)
                .forEach(a -> λ.put(a[0].toUpperCase(Locale.ROOT), Double.parseDouble(a[1])));

        Map<String, Double> rampStep = parseDoubleMap(rampRatePairs);
        Map<String, Double> maxRate  = parseDoubleMap(maxRatePairs);

        /* ---------- connect DB ---------- */
        DatabaseClient db = new Neo4jClient(uri, user, password, database, durationSeconds);
        db.connect();

        /* ---------- prepare all queries ---------- */
        QueryGenerator.Engine engine =
                QueryGenerator.Engine.valueOf(engineName.toUpperCase(Locale.ROOT));
        QueryGenerator gen = new QueryGenerator(db, engine);
        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries().forEach((k, v) -> qPool.put(k, new LinkedBlockingQueue<>(v)));

        /* ---------- bounded worker pool (virtual threads) ---------- */
        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                MAX_V_THREADS, MAX_V_THREADS, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_BACKLOG),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        long now = System.currentTimeMillis();
        long warmEnd  = now + Math.max(0, warmupSeconds) * 1000L;
        long measureEnd = warmEnd + Math.max(0, (durationSeconds - warmupSeconds)) * 1000L;

        /* ---------- warmup (always CLOSED) ---------- */
        if (warmupSeconds > 0) {
            System.out.println("Launching warmup CLOSED workers...");
            launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, warmEnd, true);
            long sleepMs = Math.max(0, warmEnd - System.currentTimeMillis());
            if (sleepMs > 0) Thread.sleep(sleepMs);
        }

        /* ---------- measured phase ---------- */
        if (arrivalMode == ArrivalMode.CLOSED) {
            runMeasuredClosed(db, workers, qPool, measureEnd);
        } else {
            if (rampIntervalSec > 0) {
                runMeasuredOpenWithRamping(db, workers, qPool, measureEnd, rampIntervalSec, rampStep, maxRate);
            } else {
                runMeasuredOpenSingleInterval(db, workers, qPool, measureEnd);
            }
        }

        /* ---------- shutdown & final report ---------- */
        workers.shutdown();
        workers.awaitTermination(5, TimeUnit.MINUTES);
        db.close();

        try {
            new MetricsAggregator(results,
                    Math.max(1, durationSeconds - Math.max(0, warmupSeconds)),
                    "benchmark_results.csv",
                    Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients))
                .printReport();
        } catch (IOException ioe) {
            System.err.println("Failed to write final CSV: " + ioe.getMessage());
        }

        Instant endTs = Instant.now();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(endTs.atZone(ZoneId.systemDefault()).toLocalTime()),
                human(Duration.between(startTs, endTs)));
        return 0;
    }

    /* ============================ CLOSED (measured) ============================ */
    private void runMeasuredClosed(DatabaseClient db,
                                   ThreadPoolExecutor workers,
                                   Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                   long phaseEnd) {
        launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, phaseEnd, false);
        long sleepMs = Math.max(0, phaseEnd - System.currentTimeMillis());
        if (sleepMs > 0) {
            try { Thread.sleep(sleepMs); } catch (InterruptedException ignored) {}
        }
    }

    private void launchClosedWorkers(DatabaseClient db, ThreadPoolExecutor pool,
                                     int oltp, int graph, int olap,
                                     Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                     long endMillis, boolean warm) {
        submitWorkers(db, pool, oltp,  qPool.get("OLTP"),  "OLTP",  endMillis, warm);
        submitWorkers(db, pool, graph, qPool.get("GRAPH"), "GRAPH", endMillis, warm);
        submitWorkers(db, pool, olap,  qPool.get("OLAP"),  "OLAP",  endMillis, warm);
    }

    private void submitWorkers(DatabaseClient db, ThreadPoolExecutor pool, int nThreads,
                               BlockingQueue<QueryTemplate.PreparedQuery> q, String cat,
                               long endMillis, boolean warm) {
        if (nThreads <= 0 || q == null) return;
        for (int i = 0; i < nThreads; i++) {
            pool.submit(new ClientWorker(db, q, cat, endMillis, warm, /*singleShot*/ false, results));
        }
    }

    /* ============================ OPEN (single interval) ============================ */
    private void runMeasuredOpenSingleInterval(DatabaseClient db,
                                               ThreadPoolExecutor workers,
                                               Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                               long measureEnd) {

        int seconds = Math.max(1, (int)((measureEnd - System.currentTimeMillis()) / 1000));
        System.out.printf("OPEN mode (measured): single interval of %d s%n", seconds);

        // Interval-scoped results
        List<QueryResult> intervalResults = Collections.synchronizedList(new ArrayList<>());

        long intervalStart = System.currentTimeMillis();

        List<Thread> submitters = new ArrayList<>();
        submitters.addAll(startShardedSubmitters("OLTP", λ.getOrDefault("OLTP", 0.0), workers, db, qPool.get("OLTP"),  measureEnd, intervalResults));
        submitters.addAll(startShardedSubmitters("GRAPH", λ.getOrDefault("GRAPH", 0.0), workers, db, qPool.get("GRAPH"), measureEnd, intervalResults));
        submitters.addAll(startShardedSubmitters("OLAP", λ.getOrDefault("OLAP", 0.0), workers, db, qPool.get("OLAP"),  measureEnd, intervalResults));

        for (Thread t : submitters) {
            try { t.join(); } catch (InterruptedException ignored) {}
        }

        // compute actual duration once
        int secs = Math.max(1, (int)Math.ceil(intervalDurationSeconds(intervalStart, measureEnd)));

        // queue pressure snapshot
        System.out.printf("workers: active=%d queued=%d%n", workers.getActiveCount(), workers.getQueue().size());

        printIntervalReport("interval-01 (OPEN single)", intervalResults, secs, "benchmark_results_interval01.csv");
        results.addAll(intervalResults);
    }

    /* ============================ OPEN (ramping) ============================ */
    private void runMeasuredOpenWithRamping(DatabaseClient db,
                                            ThreadPoolExecutor workers,
                                            Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                            long measureEnd,
                                            int intervalSec,
                                            Map<String, Double> rampStep,
                                            Map<String, Double> maxRate) {
        long now = System.currentTimeMillis();
        int totalSecs = (int) Math.max(1, (measureEnd - now) / 1000);

        int intervals = Math.max(1, totalSecs / intervalSec);
        int remainder = totalSecs % intervalSec;
        if (remainder > 0) intervals += 1;

        System.out.printf("OPEN mode (measured): %d interval(s) of %d s%n", intervals, intervalSec);

        // Working copy of λ we mutate each interval
        Map<String, Double> current = new HashMap<>(λ);

        for (int idx = 1; idx <= intervals; idx++) {
            long intervalStart = System.currentTimeMillis();
            long plannedEnd = intervalStart + intervalSec * 1000L;
            long intervalEnd = Math.min(plannedEnd, measureEnd);

            // The last interval might be shorter
            int actualSecs = Math.max(1, (int) Math.ceil(intervalDurationSeconds(intervalStart, intervalEnd)));

            String intervalName = String.format(Locale.ROOT,
                    "interval-%02d (OPEN, λ OLTP=%s GRAPH=%s OLAP=%s)",
                    idx,
                    fmtRate(current.get("OLTP")),
                    fmtRate(current.get("GRAPH")),
                    fmtRate(current.get("OLAP")));

            System.out.printf("Launching %s OPEN submitters...%n", intervalName);

            // Fresh interval results
            List<QueryResult> intervalResults = Collections.synchronizedList(new ArrayList<>());

            // Start submitters
            List<Thread> submitters = new ArrayList<>();
            submitters.addAll(startShardedSubmitters("OLTP", current.getOrDefault("OLTP", 0.0), workers, db, qPool.get("OLTP"),  intervalEnd, intervalResults));
            submitters.addAll(startShardedSubmitters("GRAPH", current.getOrDefault("GRAPH", 0.0), workers, db, qPool.get("GRAPH"), intervalEnd, intervalResults));
            submitters.addAll(startShardedSubmitters("OLAP", current.getOrDefault("OLAP", 0.0), workers, db, qPool.get("OLAP"),  intervalEnd, intervalResults));

            for (Thread t : submitters) {
                try { t.join(); } catch (InterruptedException ignored) {}
            }

            System.out.printf("%n--- Interval Completed: %s ---%n", intervalName);
            System.out.printf("workers: active=%d queued=%d%n", workers.getActiveCount(), workers.getQueue().size());

            // CSV file per interval
            String csvName = "benchmark_results_" + intervalName.replaceAll("\\s+","_").replaceAll("[()=,]","") + ".csv";

            // Interval-only report
            printIntervalReport(intervalName, intervalResults, actualSecs, csvName);

            // Add to global accumulation for the final summary
            results.addAll(intervalResults);

            // Bump rates for next interval (if any)
            if (idx < intervals) {
                bumpRate(current, rampStep, maxRate);
            }
        }
    }

    /* ============================ utilities ============================ */

    private static String fmtRate(Double d) {
        if (d == null) return "0.0";
        if (d < 1.0) return String.format(Locale.ROOT, "%.2f", d); // OLAP often fractional
        return String.format(Locale.ROOT, "%.0f", d);
    }

    private static double intervalDurationSeconds(long startMs, long endMs) {
        return Math.max(0.001, (endMs - startMs) / 1000.0);
    }

    private void bumpRate(Map<String, Double> current,
                          Map<String, Double> step,
                          Map<String, Double> maxRate) {
        for (String cat : List.of("OLTP", "GRAPH", "OLAP")) {
            double base = current.getOrDefault(cat, 0.0);
            double inc  = step.getOrDefault(cat, 0.0);
            double next = base + inc;
            double cap  = maxRate.getOrDefault(cat, Double.POSITIVE_INFINITY);
            current.put(cat, Math.min(next, cap));
        }
    }

    private void printIntervalReport(String intervalName,
                                     List<QueryResult> intervalResults,
                                     int actualIntervalSecs,
                                     String csvName) {
        try {
            new MetricsAggregator(
                    intervalResults,
                    Math.max(1, actualIntervalSecs),
                    csvName, // write CSV per interval
                    Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients)
            ).printReport();
        } catch (IOException ioe) {
            System.err.println("Failed to write interval CSV (" + intervalName + "): " + ioe.getMessage());
        }
    }

    private List<Thread> startShardedSubmitters(String cat, double lambda, ThreadPoolExecutor pool,
                                                DatabaseClient db, BlockingQueue<QueryTemplate.PreparedQuery> q,
                                                long intervalEnd,
                                                List<QueryResult> intervalResults) {
        List<Thread> threads = new ArrayList<>();
        if (lambda <= 0.0 || q == null) return threads;

        int shards = (int) Math.ceil(lambda / MAX_SUBMITTER_QPS);
        double λPerShard = lambda / shards;
        System.out.printf("Submitter %s will run at %.0f qps × %d shard(s)%n", cat, λPerShard, shards);

        for (int i = 0; i < shards; i++) {
            Thread t = Thread.startVirtualThread(
                    new ArrivalSubmitter(λPerShard, pool, db, q, cat, intervalEnd, intervalResults));
            threads.add(t);
        }
        return threads;
    }

    private static String human(Duration d) {
        return String.format("%d:%02d.%03d", d.toMinutes(), d.toSecondsPart(), d.toMillisPart());
    }
}
