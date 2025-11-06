package com.benchmark;

import com.benchmark.arrival.ArrivalMode;
import com.benchmark.arrival.ArrivalSubmitter;
import com.benchmark.client.ClientWorker;
import com.benchmark.client.DatabaseClient;
import com.benchmark.client.Neo4jClient;
import com.benchmark.client.MemgraphClient;
import com.benchmark.client.JanusGraphClient;
import com.benchmark.freshness.HeartbeatService;
import com.benchmark.generator.QueryGenerator;
import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.MetricsAggregator;
import com.benchmark.metrics.QueryResult;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

@Command(
    name = "benchmark-runner",
    mixinStandardHelpOptions = true,
    description = "HTAP benchmark – CLOSED (token) and OPEN (Poisson) arrival modes with optional ramping + freshness (HB)"
)
public class BenchmarkRunner implements Callable<Integer> {

    /* ---------------- DB flags ---------------- */
    @Option(names = "--uri",      defaultValue = "bolt://localhost:7687") String uri;
    @Option(names = "--user",     defaultValue = "neo4j")                 String user;
    @Option(names = "--password", defaultValue = "neo4j")                 String password;
    @Option(names = "--database", defaultValue = "neo4j")                 String database;

    /* ---------------- worker counts (CLOSED) ---------------- */
    @Option(names = "--oltp-clients",  defaultValue = "8") int oltpClients;
    @Option(names = "--graph-clients", defaultValue = "1") int graphClients;
    @Option(names = "--olap-clients",  defaultValue = "1") int olapClients;

    /* ---------------- OPEN arrival rates ---------------- */
    @Option(names = "--arrival-mode", defaultValue = "CLOSED") ArrivalMode arrivalMode;
    @Option(names = "--arrival-rate", arity = "1..*", split = ",",
            description = "Per-category λ, e.g. OLTP=2000,GRAPH=50,OLAP=0.1 (OPEN mode only)")
    List<String> arrivalPairs = new ArrayList<>();

    /* ---------------- Duration ---------------- */
    @Option(names = {"-d", "--duration"}, defaultValue = "180") int durationSeconds;
    @Option(names = {"-w", "--warmup"},   defaultValue = "10")  int warmupSeconds;

    /* ---------------- Ramping (OPEN) ---------------- */
    @Option(names="--ramp-interval", defaultValue="0",
            description = "Seconds between ramps in OPEN mode (0 = no ramping)")
    int rampIntervalSec;

    @Option(names="--ramp-rate-step", arity="0..*", split=",",
            description = "Per-interval λ increment per category (OPEN mode)")
    List<String> rampRatePairs = new ArrayList<>();

    @Option(names="--max-arrival-rate", arity="0..*", split=",",
            description = "Max λ per category (OPEN mode)")
    List<String> maxRatePairs = new ArrayList<>();

    /* ---------------- Engine ---------------- */
    @Option(names="--engine", defaultValue = "NEO4J",
            description = "Query engine: NEO4J | MEMGRAPH | JANUSGRAPH")
    String engineName;

    /* ---------------- JanusGraph tuning ---------------- */
    @Option(names="--janus-min-pool", defaultValue = "8")   int janusMinPool;
    @Option(names="--janus-max-pool", defaultValue = "32")  int janusMaxPool;
    @Option(names="--janus-wait-ms",  defaultValue = "30000") int janusWaitMs;
    @Option(names="--janus-timeout-sec", defaultValue = "120") int janusTimeoutSec;

    /* ---------------- Heartbeat (freshness) ---------------- */
    @Option(names="--hb-enabled", defaultValue = "true", description = "Enable heartbeat freshness") boolean hbEnabled;
    @Option(names="--hb-write-ms", defaultValue = "2000", description = "Heartbeat write interval ms") long hbWriteMs;
    @Option(names="--hb-read-ms",  defaultValue = "2000", description = "Heartbeat read interval ms")  long hbReadMs;
    @Option(names="--hb-month",    defaultValue = "2011-01", description = "Heartbeat window month YYYY-MM") String hbMonthStr;

    /* ---------------- constants ---------------- */
    private static final int MAX_V_THREADS     = 512;
    private static final int MAX_BACKLOG       = 20_000;
    private static final int MAX_SUBMITTER_QPS = 300;

    /* ---------------- internal ---------------- */
    private final Map<String, Double> λ = new HashMap<>();
    private final List<QueryResult>   results = Collections.synchronizedList(new ArrayList<>());

    /* =============================== helpers =============================== */

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

        arrivalPairs.stream()
                .map(s -> s.split("="))
                .filter(a -> a.length == 2)
                .forEach(a -> λ.put(a[0].toUpperCase(Locale.ROOT), Double.parseDouble(a[1])));

        Map<String, Double> rampStep = parseDoubleMap(rampRatePairs);
        Map<String, Double> maxRate  = parseDoubleMap(maxRatePairs);

        /* ---------- connect DB ---------- */
        DatabaseClient db = createClient(this);
        db.connect();

        /* ---------- prepare queries ---------- */
        QueryGenerator.Engine engine = QueryGenerator.Engine.valueOf(engineName.toUpperCase(Locale.ROOT));
        QueryGenerator gen = new QueryGenerator(db, engine);
        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries().forEach((k, v) -> qPool.put(k, new LinkedBlockingQueue<>(v)));

        /* ---------- HB (freshness) ---------- */
        YearMonth hbMonth = YearMonth.parse(hbMonthStr);
        HeartbeatService hb = new HeartbeatService(
                db,
                new HeartbeatService.Config(hbEnabled, hbWriteMs, hbReadMs, hbMonth, engine)
        );
        hb.start(); // starts tiny background writer + sampler (excluded from throughput)

        /* ---------- workers ---------- */
        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                MAX_V_THREADS, MAX_V_THREADS, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_BACKLOG),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        long now = System.currentTimeMillis();
        long warmEnd    = now + Math.max(0, warmupSeconds) * 1000L;
        long measureEnd = warmEnd + Math.max(0, (durationSeconds - warmupSeconds)) * 1000L;

        /* ---------- warmup ---------- */
        if (warmupSeconds > 0) {
            System.out.println("Launching warmup CLOSED workers...");
            launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, warmEnd, true);
            long sleepMs = Math.max(0, warmEnd - System.currentTimeMillis());
            if (sleepMs > 0) Thread.sleep(sleepMs);
        }

        /* ---------- measured ---------- */
        if (arrivalMode == ArrivalMode.CLOSED) {
            runMeasuredClosed(db, workers, qPool, measureEnd);
        } else {
            if (rampIntervalSec > 0) {
                runMeasuredOpenWithRamping(db, workers, qPool, measureEnd, rampIntervalSec, rampStep, maxRate);
            } else {
                runMeasuredOpenSingleInterval(db, workers, qPool, measureEnd);
            }
        }

        /* ---------- shutdown ---------- */
        workers.shutdown();
        workers.awaitTermination(5, TimeUnit.MINUTES);
        hb.stopAndReport();  // print + write hb CSV
        db.close();

        try {
            new com.benchmark.metrics.MetricsAggregator(
                    results,
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

    private static DatabaseClient createClient(BenchmarkRunner cli) {
        switch (cli.engineName.toUpperCase(Locale.ROOT)) {
            case "NEO4J":
                return new Neo4jClient(cli.uri, cli.user, cli.password, cli.database, cli.durationSeconds);
            case "MEMGRAPH":
                return new MemgraphClient(cli.uri, cli.user, cli.password);
            case "JANUSGRAPH": {
                String host; int port;
                String u = cli.uri.replace("gremlin://", "").replace("ws://", "").replace("wss://", "");
                if (u.contains(":")) {
                    String[] hp = u.split(":", 2);
                    host = hp[0]; port = Integer.parseInt(hp[1]);
                } else { host = u; port = 8182; }
                return new JanusGraphClient(host, port,
                        cli.janusMinPool, cli.janusMaxPool, cli.janusWaitMs, cli.janusTimeoutSec);
            }
            default:
                throw new IllegalArgumentException("Unknown --engine: " + cli.engineName);
        }
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

        System.out.printf("OPEN mode (measured): single interval of %d s%n",
                Math.max(1, (measureEnd - System.currentTimeMillis()) / 1000));

        List<QueryResult> intervalResults = Collections.synchronizedList(new ArrayList<>());

        List<Thread> submitters = new ArrayList<>();
        submitters.addAll(startShardedSubmitters("OLTP", λ.getOrDefault("OLTP", 0.0), workers, db, qPool.get("OLTP"),  measureEnd, intervalResults));
        submitters.addAll(startShardedSubmitters("GRAPH", λ.getOrDefault("GRAPH", 0.0), workers, db, qPool.get("GRAPH"), measureEnd, intervalResults));
        submitters.addAll(startShardedSubmitters("OLAP", λ.getOrDefault("OLAP", 0.0), workers, db, qPool.get("OLAP"),  measureEnd, intervalResults));

        for (Thread t : submitters) {
            try { t.join(); } catch (InterruptedException ignored) {}
        }

        int secs = Math.max(1, (int) Math.ceil(intervalDurationSeconds(System.currentTimeMillis(), measureEnd)));
        printIntervalReport("interval-01 (OPEN single)", intervalResults, secs);
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

        Map<String, Double> current = new HashMap<>(λ);

        for (int idx = 1; idx <= intervals; idx++) {
            long intervalStart = System.currentTimeMillis();
            long plannedEnd = intervalStart + intervalSec * 1000L;
            long intervalEnd = Math.min(plannedEnd, measureEnd);
            int actualSecs = Math.max(1, (int) Math.ceil(intervalDurationSeconds(intervalStart, intervalEnd)));

            String intervalName = String.format(Locale.ROOT,
                    "interval-%02d (OPEN, λ OLTP=%s GRAPH=%s OLAP=%s)",
                    idx, fmtRate(current.get("OLTP")), fmtRate(current.get("GRAPH")), fmtRate(current.get("OLAP")));

            System.out.printf("Launching %s OPEN submitters...%n", intervalName);

            List<QueryResult> intervalResults = Collections.synchronizedList(new ArrayList<>());

            List<Thread> submitters = new ArrayList<>();
            submitters.addAll(startShardedSubmitters("OLTP", current.getOrDefault("OLTP", 0.0), workers, db, qPool.get("OLTP"),  intervalEnd, intervalResults));
            submitters.addAll(startShardedSubmitters("GRAPH", current.getOrDefault("GRAPH", 0.0), workers, db, qPool.get("GRAPH"), intervalEnd, intervalResults));
            submitters.addAll(startShardedSubmitters("OLAP", current.getOrDefault("OLAP", 0.0), workers, db, qPool.get("OLAP"),  intervalEnd, intervalResults));

            for (Thread t : submitters) {
                try { t.join(); } catch (InterruptedException ignored) {}
            }

            System.out.printf("%n--- Interval Completed: %s ---%n%n", intervalName);
            printIntervalReport(intervalName, intervalResults, actualSecs);
            results.addAll(intervalResults);

            if (idx < intervals) bumpRate(current, rampStep, maxRate);
        }
    }

    /* ============================ utilities ============================ */

    private static String fmtRate(Double d) {
        if (d == null) return "0.0";
        if (d < 1.0) return String.format(Locale.ROOT, "%.2f", d);
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
                                     int actualSecs) {
        String safe = intervalName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");
        if (safe.startsWith("-")) safe = safe.substring(1);
        if (safe.endsWith("-"))  safe = safe.substring(0, safe.length()-1);
        String csvFile = "interval-" + safe + ".csv";

        try {
            new MetricsAggregator(
                    intervalResults,
                    Math.max(1, actualSecs),
                    csvFile,
                    Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients)
            ).printReport();
        } catch (IOException ioe) {
            System.err.println("Failed to write interval CSV (" + csvFile + "): " + ioe.getMessage());
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
