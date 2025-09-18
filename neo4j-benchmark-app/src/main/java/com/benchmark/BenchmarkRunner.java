// neo4j-benchmark-app/src/main/java/com/benchmark/BenchmarkRunner.java
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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Neo4j HTAP benchmark – CLOSED (token) and OPEN (Poisson) arrival modes.
 * Virtual threads (JDK 21). Supports periodic ramp-up in both modes.
 */
@Command(
        name = "benchmark-runner",
        mixinStandardHelpOptions = true,
        description = "Neo4j HTAP benchmark – open / closed arrival modes (with optional ramp-up)"
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

    // OPEN-loop arrival rates (QPS), e.g. --arrival-rate OLTP=2000,GRAPH=50,OLAP=0.1
    @Option(names = "--arrival-mode", defaultValue = "CLOSED") ArrivalMode arrivalMode;
    @Option(names = "--arrival-rate", arity = "1..*", split = ",") List<String> arrivalPairs = new ArrayList<>();

    @Option(names = {"-d", "--duration"}, defaultValue = "180") int durationSeconds;
    @Option(names = {"-w", "--warmup"},   defaultValue = "10")  int warmupSeconds;

    /* -------- ramp-up controls (optional) -------- */
    // Period between ramps (applies to both OPEN and CLOSED). 0 = disabled.
    @Option(names="--ramp-interval", defaultValue="0",
            description = "Seconds between ramps (0 = disabled)")
    int rampIntervalSec;

    // OPEN mode: per-interval lambda increment per category (additive). Example: OLTP=250,GRAPH=5,OLAP=0.02
    @Option(names="--ramp-rate-step", arity="0..*", split=",",
            description = "Per-interval λ addend per category (OPEN mode)")
    List<String> rampRatePairs = new ArrayList<>();

    // OPEN mode: max lambda clamp per category. Example: OLTP=4000,GRAPH=100
    @Option(names="--max-arrival-rate", arity="0..*", split=",",
            description = "Max λ per category (OPEN mode)")
    List<String> maxRatePairs = new ArrayList<>();

    // CLOSED mode: per-interval client increment per category (additive). Example: OLTP=2,GRAPH=0,OLAP=0
    @Option(names="--ramp-clients-step", arity="0..*", split=",",
            description = "Per-interval client increment per category (CLOSED mode)")
    List<String> rampClientPairs = new ArrayList<>();

    // CLOSED mode: max clients clamp per category. Example: OLTP=64,GRAPH=2,OLAP=2
    @Option(names="--max-clients", arity="0..*", split=",",
            description = "Max clients per category (CLOSED mode)")
    List<String> maxClientsPairs = new ArrayList<>();

    /* ---------------- constants ---------------- */
    private static final int MAX_V_THREADS     = 512;      // cap concurrency of worker pool
    private static final int MAX_BACKLOG       = 20_000;   // queue capacity (OPEN mode safety valve)
    private static final int MAX_SUBMITTER_QPS = 300;      // per-shard cap for ArrivalSubmitter

    /* ---------------- internal ---------------- */
    private final Map<String, Double> λ = new HashMap<>(); // requested arrival rates (OPEN)
    private final List<QueryResult>    results =
            Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    /* ================================================================ */
    @Override
    public Integer call() throws Exception {
        Instant startTs = Instant.now();
        System.out.printf("Benchmark started at %s%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(
                        startTs.atZone(ZoneId.systemDefault()).toLocalTime()));

        /* ---------- parse λ from CLI ---------- */
        parseDoubleMap(arrivalPairs, λ);

        /* ---------- connect DB ---------- */
        DatabaseClient db = new Neo4jClient(uri, user, password, database, durationSeconds);
        db.connect();

        /* ---------- prepare queries ---------- */
        QueryGenerator gen = new QueryGenerator(db);
        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries().forEach((k, v) -> qPool.put(k, new LinkedBlockingQueue<>(v)));

        /* ---------- worker pool (bounded) ---------- */
        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                MAX_V_THREADS, MAX_V_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_BACKLOG),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        // Live probe only in OPEN mode
        ScheduledExecutorService probe = (arrivalMode == ArrivalMode.OPEN)
                ? Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory())
                : null;
        long phaseStartMillis = System.currentTimeMillis();
        if (probe != null) {
            probe.scheduleAtFixedRate(() -> {
                int queued = workers.getQueue().size();
                int active = workers.getActiveCount();
                long since = (System.currentTimeMillis() - phaseStartMillis) / 1000;
                int backlog = Math.max(0, queued + active - MAX_V_THREADS);
                System.out.printf("t+%3ds backlog=%7d  queued=%6d  active=%4d%n",
                        since, backlog, queued, active);
            }, 0, 1, TimeUnit.SECONDS);
        }

        long warmEnd  = System.currentTimeMillis() + warmupSeconds * 1000L;
        long phaseEnd = warmEnd + (durationSeconds - warmupSeconds) * 1000L;

        /* ---------- Phase A – warm-up (CLOSED tokens) ---------- */
        launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, warmEnd, true);
        // Wait precisely for warm-up to finish
        long now = System.currentTimeMillis();
        if (now < warmEnd) Thread.sleep(warmEnd - now);

        /* ---------- Phase B – measured ---------- */
        if (arrivalMode == ArrivalMode.CLOSED) {
            runClosedMeasuredWithOptionalRamp(db, workers, qPool, phaseEnd);
        } else {
            runOpenMeasuredWithOptionalRamp(db, workers, qPool, warmEnd, phaseEnd);
        }

        if (probe != null) probe.shutdownNow();

        workers.shutdown();
        workers.awaitTermination(5, TimeUnit.MINUTES);
        db.close();

        /* ---------- report ---------- */
        new MetricsAggregator(
                results,
                durationSeconds - warmupSeconds,
                "benchmark_results.csv",
                Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients)
        ).printReport();

        Instant endTs = Instant.now();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(
                        endTs.atZone(ZoneId.systemDefault()).toLocalTime()),
                human(Duration.between(startTs, endTs)));
        return 0;
    }

    /* ========================= CLOSED mode (measured) ========================= */
    private void runClosedMeasuredWithOptionalRamp(DatabaseClient db,
                                                   ThreadPoolExecutor pool,
                                                   Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                                   long phaseEnd) throws InterruptedException {

        if (rampIntervalSec <= 0) {
            // No ramping: launch fixed clients for the whole phase.
            launchClosedWorkers(db, pool, oltpClients, graphClients, olapClients, qPool, phaseEnd, false);
            return;
        }

        // Ramp settings
        Map<String, Integer> rampClients = new HashMap<>();
        Map<String, Integer> maxClients  = new HashMap<>();
        parseIntMap(rampClientPairs, rampClients);
        parseIntMap(maxClientsPairs,  maxClients);

        // Current client counts (start at initial)
        Map<String, Integer> cur = new HashMap<>(Map.of(
                "OLTP", oltpClients,
                "GRAPH", graphClients,
                "OLAP", olapClients
        ));

        // Launch initial clients
        launchClosedWorkers(db, pool, cur.get("OLTP"), cur.get("GRAPH"), cur.get("OLAP"), qPool, phaseEnd, false);

        // Periodically add more, clamped by maxClients
        while (System.currentTimeMillis() < phaseEnd) {
            long sleepMs = Math.min(phaseEnd - System.currentTimeMillis(), rampIntervalSec * 1000L);
            if (sleepMs > 0) Thread.sleep(sleepMs);
            if (System.currentTimeMillis() >= phaseEnd) break;

            // For each category, compute additions
            rampClosedCategory(db, pool, qPool, phaseEnd, cur, rampClients, maxClients, "OLTP");
            rampClosedCategory(db, pool, qPool, phaseEnd, cur, rampClients, maxClients, "GRAPH");
            rampClosedCategory(db, pool, qPool, phaseEnd, cur, rampClients, maxClients, "OLAP");
        }
    }

    private void rampClosedCategory(DatabaseClient db,
                                    ThreadPoolExecutor pool,
                                    Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                    long phaseEnd,
                                    Map<String, Integer> cur,
                                    Map<String, Integer> rampClients,
                                    Map<String, Integer> maxClients,
                                    String cat) {
        int step = rampClients.getOrDefault(cat, 0);
        if (step <= 0) return;
        int max  = maxClients.getOrDefault(cat, Integer.MAX_VALUE);
        int have = cur.getOrDefault(cat, 0);
        int toAdd = Math.max(0, Math.min(step, max - have));
        if (toAdd <= 0) return;

        cur.put(cat, have + toAdd);
        System.out.printf("RAMP (CLOSED): +%d %s clients (total=%d, max=%s)%n",
                toAdd, cat, cur.get(cat), (max == Integer.MAX_VALUE ? "∞" : String.valueOf(max)));

        submitWorkers(db, pool, toAdd, qPool.get(cat), cat, phaseEnd, false);
    }

    /* =========================== OPEN mode (measured) =========================== */
    private void runOpenMeasuredWithOptionalRamp(DatabaseClient db,
                                                 ThreadPoolExecutor pool,
                                                 Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                                 long phaseStart,
                                                 long phaseEnd) throws InterruptedException {

        if (rampIntervalSec <= 0) {
            // Single-configuration run
            launchOpenSubmittersChunk(db, pool, qPool, phaseEnd, λ);
            return;
        }

        // Ramp settings
        Map<String, Double> rateStep = new HashMap<>();
        Map<String, Double> rateMax  = new HashMap<>();
        parseDoubleMap(rampRatePairs, rateStep);
        parseDoubleMap(maxRatePairs,  rateMax);

        // Current lambdas (start at requested λ)
        Map<String, Double> cur = new HashMap<>();
        cur.put("OLTP", λ.getOrDefault("OLTP", 0.0));
        cur.put("GRAPH", λ.getOrDefault("GRAPH", 0.0));
        cur.put("OLAP", λ.getOrDefault("OLAP", 0.0));

        long chunkStart = Math.max(phaseStart, System.currentTimeMillis());
        while (chunkStart < phaseEnd) {
            long chunkEnd = Math.min(phaseEnd, chunkStart + rampIntervalSec * 1000L);

            // Run one chunk at the current λ snapshot
            launchOpenSubmittersChunk(db, pool, qPool, chunkEnd, cur);

            // Wait until chunk end
            long now = System.currentTimeMillis();
            if (now < chunkEnd) Thread.sleep(chunkEnd - now);

            if (chunkEnd >= phaseEnd) break; // done

            // Step up λ for next chunk
            cur.put("OLTP", clamp(cur.getOrDefault("OLTP", 0.0)  + rateStep.getOrDefault("OLTP",  0.0),
                    0.0, rateMax.getOrDefault("OLTP",  Double.POSITIVE_INFINITY)));
            cur.put("GRAPH", clamp(cur.getOrDefault("GRAPH", 0.0) + rateStep.getOrDefault("GRAPH", 0.0),
                    0.0, rateMax.getOrDefault("GRAPH", Double.POSITIVE_INFINITY)));
            cur.put("OLAP", clamp(cur.getOrDefault("OLAP", 0.0)  + rateStep.getOrDefault("OLAP",  0.0),
                    0.0, rateMax.getOrDefault("OLAP",  Double.POSITIVE_INFINITY)));

            chunkStart = chunkEnd;
        }
    }

    /** Run a single OPEN chunk (from now until chunkEndMillis) using given λ per category. */
    private void launchOpenSubmittersChunk(DatabaseClient db,
                                           ThreadPoolExecutor pool,
                                           Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                           long chunkEndMillis,
                                           Map<String, Double> lambdaByCat) throws InterruptedException {
        List<Thread> submitters = new ArrayList<>();
        submitters.addAll(startShardedSubmitters("OLTP",  lambdaByCat.getOrDefault("OLTP",  0.0), pool, db, qPool.get("OLTP"),  chunkEndMillis));
        submitters.addAll(startShardedSubmitters("GRAPH", lambdaByCat.getOrDefault("GRAPH", 0.0), pool, db, qPool.get("GRAPH"), chunkEndMillis));
        submitters.addAll(startShardedSubmitters("OLAP",  lambdaByCat.getOrDefault("OLAP",  0.0), pool, db, qPool.get("OLAP"),  chunkEndMillis));
        for (Thread t : submitters) t.join();
    }

    /* ============================== shared helpers ============================== */
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
        if (q == null || nThreads <= 0) return;
        for (int i = 0; i < nThreads; i++) {
            pool.submit(new ClientWorker(
                    db, q, cat, endMillis, warm,
                    /* singleShot */ false,
                    results));
        }
    }

    private List<Thread> startShardedSubmitters(String cat, double lambda,
                                                ThreadPoolExecutor pool,
                                                DatabaseClient db,
                                                BlockingQueue<QueryTemplate.PreparedQuery> q,
                                                long endMillis) {
        List<Thread> threads = new ArrayList<>();
        if (q == null || lambda <= 0.0) return threads;

        int shards = (int) Math.ceil(lambda / MAX_SUBMITTER_QPS);
        double λPerShard = lambda / shards;
        double secs = Math.max(0.0, (endMillis - System.currentTimeMillis()) / 1000.0);
        System.out.printf("Submitter %s will run at %.0f qps × %d shard(s) for %.2f s%n",
                cat, λPerShard, shards, secs);

        for (int i = 0; i < shards; i++) {
            Thread t = Thread.startVirtualThread(
                    new ArrivalSubmitter(λPerShard, pool, db, q, cat, endMillis, results));
            threads.add(t);
        }
        return threads;
    }

    private static void parseDoubleMap(List<String> pairs, Map<String, Double> out) {
        for (String s : pairs) {
            if (s == null || s.isBlank()) continue;
            String[] a = s.split("=", 2);
            if (a.length == 2) out.put(a[0].trim().toUpperCase(Locale.ROOT), Double.parseDouble(a[1].trim()));
        }
    }
    private static void parseIntMap(List<String> pairs, Map<String, Integer> out) {
        for (String s : pairs) {
            if (s == null || s.isBlank()) continue;
            String[] a = s.split("=", 2);
            if (a.length == 2) out.put(a[0].trim().toUpperCase(Locale.ROOT), Integer.parseInt(a[1].trim()));
        }
    }
    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }
    private static String human(Duration d) {
        return String.format("%d:%02d.%03d",
                d.toMinutes(), d.toSecondsPart(), d.toMillisPart());
    }
}
