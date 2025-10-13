// neo4j-benchmark-app/src/main/java/com/benchmark/BenchmarkRunner.java
// ------------------------------------------------------------------
// Ramping support (OPEN & CLOSED) + per-interval reports.
// - New CLI flags:
//     --ramp-interval <sec>                (0 = disabled; single interval)
//     --ramp-rate-step  OLTP=250,GRAPH=5   (OPEN) per-interval λ increment
//     --max-arrival-rate OLTP=4000,GRAPH=0 (OPEN) clamp per category
//     --ramp-clients-step OLTP=2,GRAPH=0   (CLOSED) per-interval client increment
//     --max-clients OLTP=64,GRAPH=2        (CLOSED) clamp per category
//
// - OPEN mode: splits measured phase into intervals; each interval (re)launches
//   sharded submitters at the current λ_i and stops them at interval end.
// - CLOSED mode: splits measured phase into intervals; each interval launches
//   the computed number of workers and stops them at interval end.
// - Prints a report at the end of each measured interval and a final report.
//
// Notes:
// * CSV writing is wrapped in try/catch (IOException).
// * Keeps your existing ArrivalSubmitter & ClientWorker contracts.

package com.benchmark;

import com.benchmark.arrival.ArrivalMode;
import com.benchmark.arrival.ArrivalSubmitter;
import com.benchmark.client.*;
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

@Command(name="benchmark-runner", mixinStandardHelpOptions = true,
         description = "Neo4j/Memgraph/JanusGraph HTAP benchmark – open/closed modes (with ramping)")
public class BenchmarkRunner implements Callable<Integer> {

    /* ---------------- DB selection ---------------- */
    @Option(names="--dbms", defaultValue="NEO4J",
            description="Target DBMS: NEO4J | MEMGRAPH | JANUSGRAPH")
    String dbms;

    // Neo4j / Memgraph
    @Option(names="--uri",      defaultValue="bolt://localhost:7687") String uri;
    @Option(names="--user",     defaultValue="neo4j")                 String user;
    @Option(names="--password", defaultValue="neo4j")                 String password;
    @Option(names="--database", defaultValue="neo4j")                 String database;

    // JanusGraph (Gremlin Server)
    @Option(names="--gremlin-host", defaultValue="localhost") String gremlinHost;
    @Option(names="--gremlin-port", defaultValue="8182")      int gremlinPort;

    /* ---------------- Workload sizing ---------------- */
    // CLOSED mode clients (initial values)
    @Option(names="--oltp-clients",  defaultValue="8") int oltpClients;
    @Option(names="--graph-clients", defaultValue="1") int graphClients;
    @Option(names="--olap-clients",  defaultValue="1") int olapClients;

    // OPEN mode arrival rates (initial λ)
    @Option(names="--arrival-mode", defaultValue="CLOSED") ArrivalMode arrivalMode;
    @Option(names="--arrival-rate", arity="1..*", split=",") List<String> arrivalPairs = new ArrayList<>();

    /* ---------------- Timing ---------------- */
    @Option(names={"-d","--duration"}, defaultValue="180") int durationSeconds;
    @Option(names={"-w","--warmup"},   defaultValue="10")  int warmupSeconds;

    /* ---------------- Ramping controls ---------------- */
    @Option(names="--ramp-interval", defaultValue="0",
            description="Seconds between ramps (0 = disabled; one interval)")
    int rampIntervalSec;

    // OPEN mode: add to λ every ramp interval
    @Option(names="--ramp-rate-step", arity="0..*", split=",",
            description="Per-interval λ increment per category (OPEN mode). Example: OLTP=250,GRAPH=5")
    List<String> rampRatePairs = new ArrayList<>();

    @Option(names="--max-arrival-rate", arity="0..*", split=",",
            description="Max λ per category (OPEN mode). Example: OLTP=4000,GRAPH=100")
    List<String> maxRatePairs = new ArrayList<>();

    // CLOSED mode: add to client counts every ramp interval
    @Option(names="--ramp-clients-step", arity="0..*", split=",",
            description="Per-interval client increment per category (CLOSED mode). Example: OLTP=2,GRAPH=0")
    List<String> rampClientPairs = new ArrayList<>();

    @Option(names="--max-clients", arity="0..*", split=",",
            description="Max clients per category (CLOSED mode). Example: OLTP=64,GRAPH=2,OLAP=2")
    List<String> maxClientsPairs = new ArrayList<>();

    /* ---------------- constants ---------------- */
    private static final int MAX_V_THREADS     = 512;
    private static final int MAX_BACKLOG       = 20_000;
    private static final int MAX_SUBMITTER_QPS = 300;   // per shard (open mode)

    /* ---------------- internal state ---------------- */
    private final Map<String, Double> λ0 = new HashMap<>();   // initial λ from CLI
    private final List<QueryResult> results =
            Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    /* ================================================================ */
    @Override
    public Integer call() throws Exception {
        Instant start = Instant.now();
        System.out.printf("Benchmark started at %s%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(start.atZone(ZoneId.systemDefault()).toLocalTime()));

        // Parse initial λ
        arrivalPairs.stream()
                .map(s -> s.split("="))
                .filter(a -> a.length == 2)
                .forEach(a -> λ0.put(a[0].trim().toUpperCase(Locale.ROOT), Double.parseDouble(a[1].trim())));

        Map<String, Double> rampRate = parseDoubleMap(rampRatePairs);
        Map<String, Double> maxRate  = parseDoubleMap(maxRatePairs);
        Map<String, Integer> rampClients = parseIntMap(rampClientPairs);
        Map<String, Integer> maxClients  = parseIntMap(maxClientsPairs);

        /* ---------- connect DB ---------- */
        DatabaseClient db;
        QueryGenerator.Engine engine;
        switch (dbms.toUpperCase(Locale.ROOT)) {
            case "MEMGRAPH" -> {
                db = new MemgraphClient(uri, user, password);
                engine = QueryGenerator.Engine.MEMGRAPH;
            }
            case "JANUSGRAPH" -> {
                db = new JanusGraphClient(gremlinHost, gremlinPort);
                engine = QueryGenerator.Engine.JANUSGRAPH;
            }
            default -> {
                db = new Neo4jClient(uri, user, password, database, durationSeconds);
                engine = QueryGenerator.Engine.NEO4J;
            }
        }
        db.connect();

        /* ---------- prepare queries ---------- */
        QueryGenerator gen = new QueryGenerator(db, engine);
        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries().forEach((k, v) -> qPool.put(k, new LinkedBlockingQueue<>(v)));

        /* ---------- warm-up (always CLOSED workers for warm-cache) ---------- */
        long warmEnd = System.currentTimeMillis() + warmupSeconds * 1000L;
        runClosedInterval(db, /*intervalName*/"warmup",
                oltpClients, graphClients, olapClients, qPool, warmEnd, /*warmup*/true);

        /* ---------- measured phase ---------- */
        int measuredSec = Math.max(0, durationSeconds - warmupSeconds);
        if (measuredSec == 0) {
            System.out.println("Duration equals warmup; nothing to measure.");
            db.close();
            return 0;
        }

        if (arrivalMode == ArrivalMode.CLOSED) {
            runMeasuredClosedWithRamps(db, measuredSec, qPool, rampClients, maxClients);
        } else {
            runMeasuredOpenWithRamps(db, measuredSec, qPool, rampRate, maxRate);
        }

        /* ---------- final report ---------- */
        try {
            new MetricsAggregator(results, measuredSec, "benchmark_results.csv",
                    Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients))
                    .printReport();
        } catch (IOException ioe) {
            System.err.println("[WARN] Failed to write final CSV: " + ioe.getMessage());
        }

        db.close();
        Instant end = Instant.now();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(end.atZone(ZoneId.systemDefault()).toLocalTime()),
                Duration.between(start, end));
        return 0;
    }

    /* ================================================================
       CLOSED mode (measured) with per-interval ramping of client counts
       ================================================================ */
    private void runMeasuredClosedWithRamps(DatabaseClient db,
                                            int measuredSec,
                                            Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                            Map<String, Integer> rampClients,
                                            Map<String, Integer> maxClients) throws InterruptedException {

        int intervalSec = (rampIntervalSec <= 0) ? measuredSec : rampIntervalSec;
        int intervals   = (int) Math.ceil((double) measuredSec / intervalSec);

        int baseOLTP  = oltpClients;
        int baseGRAPH = graphClients;
        int baseOLAP  = olapClients;

        for (int i = 0; i < intervals; i++) {
            long now = System.currentTimeMillis();
            long intervalEnd = now + Math.min(intervalSec, measuredSec - i * intervalSec) * 1000L;

            int addOLTP  = i * rampClients.getOrDefault("OLTP",  0);
            int addGRAPH = i * rampClients.getOrDefault("GRAPH", 0);
            int addOLAP  = i * rampClients.getOrDefault("OLAP",  0);

            int maxOLTP  = maxClients.getOrDefault("OLTP",  Integer.MAX_VALUE);
            int maxGRAPH = maxClients.getOrDefault("GRAPH", Integer.MAX_VALUE);
            int maxOLAP  = maxClients.getOrDefault("OLAP",  Integer.MAX_VALUE);

            int thisOLTP  = Math.min(baseOLTP  + addOLTP,  maxOLTP);
            int thisGRAPH = Math.min(baseGRAPH + addGRAPH, maxGRAPH);
            int thisOLAP  = Math.min(baseOLAP  + addOLAP,  maxOLAP);

            String name = String.format("interval-%02d (CLOSED, clients OLTP=%d GRAPH=%d OLAP=%d)",
                    i + 1, thisOLTP, thisGRAPH, thisOLAP);

            runClosedInterval(db, name, thisOLTP, thisGRAPH, thisOLAP, qPool, intervalEnd, /*warmup*/false);

            // Per-interval report (delta over this interval window)
            emitIntervalReport(name, intervalSec);
        }
    }

    private void runClosedInterval(DatabaseClient db, String label,
                                   int oltp, int graph, int olap,
                                   Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                   long endMillis, boolean warmup) throws InterruptedException {

        System.out.println("Launching " + label + " CLOSED workers...");
        ExecutorService oltpPool  = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        ExecutorService graphPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        ExecutorService olapPool  = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        submitWorkers(db, oltpPool,  oltp,  qPool.get("OLTP"),  "OLTP",  endMillis, warmup);
        submitWorkers(db, graphPool, graph, qPool.get("GRAPH"), "GRAPH", endMillis, warmup);
        submitWorkers(db, olapPool,  olap,  qPool.get("OLAP"),  "OLAP",  endMillis, warmup);

        long waitMs = Math.max(0L, endMillis - System.currentTimeMillis());
        Thread.sleep(waitMs);

        oltpPool.shutdown(); graphPool.shutdown(); olapPool.shutdown();
        oltpPool.awaitTermination(30, TimeUnit.SECONDS);
        graphPool.awaitTermination(30, TimeUnit.SECONDS);
        olapPool.awaitTermination(30, TimeUnit.SECONDS);
    }

    private void submitWorkers(DatabaseClient db, ExecutorService pool, int nThreads,
                               BlockingQueue<QueryTemplate.PreparedQuery> q, String cat,
                               long endMillis, boolean warm) {
        if (q == null || nThreads <= 0) return;
        for (int i = 0; i < nThreads; i++) {
            pool.submit(new ClientWorker(db, q, cat, endMillis, warm, false, results));
        }
    }

    /* ================================================================
       OPEN mode (measured) with per-interval ramping of arrival rates
       ================================================================ */
    private void runMeasuredOpenWithRamps(DatabaseClient db,
                                          int measuredSec,
                                          Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                          Map<String, Double> rampRate,
                                          Map<String, Double> maxRate) throws InterruptedException {

        int intervalSec = (rampIntervalSec <= 0) ? measuredSec : rampIntervalSec;
        int intervals   = (int) Math.ceil((double) measuredSec / intervalSec);

        System.out.println("OPEN mode (measured): " + intervals + " interval(s) of " + intervalSec + " s");

        for (int i = 0; i < intervals; i++) {
            double λOLTP  = clamp(λ0.getOrDefault("OLTP",  0.0) + i * rampRate.getOrDefault("OLTP",  0.0),
                                  maxRate.getOrDefault("OLTP",  Double.MAX_VALUE));
            double λGRAPH = clamp(λ0.getOrDefault("GRAPH", 0.0) + i * rampRate.getOrDefault("GRAPH", 0.0),
                                  maxRate.getOrDefault("GRAPH", Double.MAX_VALUE));
            double λOLAP  = clamp(λ0.getOrDefault("OLAP",  0.0) + i * rampRate.getOrDefault("OLAP",  0.0),
                                  maxRate.getOrDefault("OLAP",  Double.MAX_VALUE));

            long now = System.currentTimeMillis();
            long intervalEnd = now + Math.min(intervalSec, measuredSec - i * intervalSec) * 1000L;
            String name = String.format("interval-%02d (OPEN, λ OLTP=%.0f GRAPH=%.1f OLAP=%.2f)",
                    i + 1, λOLTP, λGRAPH, λOLAP);

            runOpenInterval(db, name, qPool, intervalEnd, λOLTP, λGRAPH, λOLAP);

            // Per-interval report (delta over this interval window)
            emitIntervalReport(name, intervalSec);
        }
    }

    private void runOpenInterval(DatabaseClient db, String label,
                                 Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                 long endMs,
                                 double λOLTP, double λGRAPH, double λOLAP) throws InterruptedException {

        System.out.println("Launching " + label + " OPEN submitters...");

        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                MAX_V_THREADS, MAX_V_THREADS, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_BACKLOG),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        List<Thread> submitters = new ArrayList<>();
        submitters.addAll(startShardedSubmitters("OLTP",  λOLTP,  workers, db, qPool.get("OLTP"),  endMs));
        submitters.addAll(startShardedSubmitters("GRAPH", λGRAPH, workers, db, qPool.get("GRAPH"), endMs));
        submitters.addAll(startShardedSubmitters("OLAP",  λOLAP,  workers, db, qPool.get("OLAP"),  endMs));

        // Wait until the interval ends
        long waitMs = Math.max(0L, endMs - System.currentTimeMillis());
        Thread.sleep(waitMs);

        // Stop submitters
        for (Thread t : submitters) {
            try { t.join(2000); } catch (InterruptedException ignored) { }
        }

        // Drain workers
        workers.shutdown();
        workers.awaitTermination(30, TimeUnit.SECONDS);
    }

    private List<Thread> startShardedSubmitters(String cat, double lambda,
                                                ThreadPoolExecutor pool, DatabaseClient db,
                                                BlockingQueue<QueryTemplate.PreparedQuery> q,
                                                long phaseEnd) {
        List<Thread> threads = new ArrayList<>();
        if (lambda <= 0.0 || q == null) return threads;
        int shards = (int) Math.ceil(lambda / MAX_SUBMITTER_QPS);
        double per = lambda / shards;
        System.out.printf("Submitter %s will run at %.0f qps × %d shard(s)%n", cat, per, shards);
        for (int i=0;i<shards;i++) {
            threads.add(Thread.startVirtualThread(
                    new ArrivalSubmitter(per, pool, db, q, cat, phaseEnd, results)));
        }
        return threads;
    }

    /* ================================================================
       Per-interval reporting
       ================================================================ */
    private void emitIntervalReport(String label, int nominalSeconds) {
        // Take a snapshot of current results list for this interval’s quick summary.
        // We don’t remove from the shared list (final report uses all measured results).
        // To keep it simple and fast, we just print a header; for CSV per interval,
        // call MetricsAggregator with a filtered slice if you prefer.
        System.out.println();
        System.out.println("--- Interval Completed: " + label + " ---");
        try {
            new MetricsAggregator(results, nominalSeconds,
                    "benchmark_results_" + label.replace(' ', '_') + ".csv",
                    Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients))
                    .printReport();
        } catch (IOException ioe) {
            System.err.println("[WARN] Failed to write CSV for " + label + ": " + ioe.getMessage());
        }
        System.out.println();
    }

    /* ================================================================ */
    private static Map<String, Double> parseDoubleMap(List<String> pairs) {
        Map<String, Double> m = new HashMap<>();
        for (String s : pairs) {
            if (s == null || s.isBlank()) continue;
            String[] a = s.split("=", 2);
            if (a.length == 2) m.put(a[0].trim().toUpperCase(Locale.ROOT), Double.parseDouble(a[1].trim()));
        }
        return m;
    }

    private static Map<String, Integer> parseIntMap(List<String> pairs) {
        Map<String, Integer> m = new HashMap<>();
        for (String s : pairs) {
            if (s == null || s.isBlank()) continue;
            String[] a = s.split("=", 2);
            if (a.length == 2) m.put(a[0].trim().toUpperCase(Locale.ROOT), Integer.parseInt(a[1].trim()));
        }
        return m;
    }

    private static double clamp(double v, double max) {
        return v > max ? max : v;
    }
}
