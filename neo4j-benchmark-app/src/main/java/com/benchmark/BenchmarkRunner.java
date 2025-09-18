// neo4j-benchmark-app/src/main/java/com/benchmark/BenchmarkRunner.java
// ------------------------------------------------------------------
// Updated 2025-09-18:
//   • Interval reporting during ramping (OPEN & CLOSED).
//   • Per-interval "workers" metadata (client count in CLOSED; shard count in OPEN).
//   • Final report over the whole measured phase.
//   • Keeps using virtual threads and the bounded worker pool.
//
// CLI examples (OPEN with ramp + interval = 30s):
//   --arrival-mode OPEN --arrival-rate OLTP=1500 --ramp-interval 30 --ramp-rate-step OLTP=250 --max-arrival-rate OLTP=4000
//
// CLI examples (CLOSED with ramp + interval = 30s):
//   --arrival-mode CLOSED --oltp-clients 8 --ramp-interval 30 --ramp-clients-step OLTP=4 --max-clients OLTP=40
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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.io.IOException;

@Command(name="benchmark-runner", mixinStandardHelpOptions = true,
        description = "Neo4j HTAP benchmark – open / closed arrival modes")
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
    @Option(names = "--arrival-rate", arity = "1..*", split = ",") List<String> arrivalPairs = new ArrayList<>();

    @Option(names = {"-d", "--duration"}, defaultValue = "180") int durationSeconds;
    @Option(names = {"-w", "--warmup"},   defaultValue = "10")  int warmupSeconds;

    // ----- Ramping knobs (optional) -----
    @Option(names="--ramp-interval", defaultValue="0",
            description = "Seconds between ramps & interval reports (0 = disabled)")
    int rampIntervalSec;

    // OPEN mode: add to λ every interval, per category.  e.g. OLTP=250,GRAPH=5,OLAP=0.02
    @Option(names="--ramp-rate-step", arity="0..*", split=",",
            description = "Per-interval λ increment per category (OPEN mode)")
    List<String> rampRatePairs = new ArrayList<>();

    // OPEN mode: clamp maxima for λ.  e.g. OLTP=4000,GRAPH=100
    @Option(names="--max-arrival-rate", arity="0..*", split=",",
            description = "Max λ per category (OPEN mode)")
    List<String> maxRatePairs = new ArrayList<>();

    // CLOSED mode: add clients every interval, per category.  e.g. OLTP=2,GRAPH=0,OLAP=0
    @Option(names="--ramp-clients-step", arity="0..*", split=",",
            description = "Per-interval client increment per category (CLOSED mode)")
    List<String> rampClientPairs = new ArrayList<>();

    // CLOSED mode: clamp maxima for clients.  e.g. OLTP=64,GRAPH=2,OLAP=2
    @Option(names="--max-clients", arity="0..*", split=",",
            description = "Max clients per category (CLOSED mode)")
    List<String> maxClientsPairs = new ArrayList<>();

    /* ---------------- constants ---------------- */
    private static final int MAX_V_THREADS     = 512;     // pool capacity for running tasks
    private static final int MAX_BACKLOG       = 20_000;  // queued tasks allowed
    private static final int MAX_SUBMITTER_QPS = 300;     // shards cap per arrival thread

    private static final String CSV_OUT = "benchmark_results.csv";

    /* ---------------- internal ---------------- */
    private final Map<String, Double> λ = new HashMap<>();
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
                DateTimeFormatter.ISO_LOCAL_TIME.format(startTs.atZone(ZoneId.systemDefault()).toLocalTime()));

        // parse OPEN λ= pairs
        arrivalPairs.stream()
                .map(s -> s.split("="))
                .filter(a -> a.length == 2)
                .forEach(a -> λ.put(a[0].trim().toUpperCase(Locale.ROOT),
                        Double.parseDouble(a[1].trim())));

        // parse ramp maps
        Map<String, Double> rampRate = parseDoubleMap(rampRatePairs);
        Map<String, Double> maxRate  = parseDoubleMap(maxRatePairs);
        Map<String, Integer> rampClients = parseIntMap(rampClientPairs);
        Map<String, Integer> maxClients  = parseIntMap(maxClientsPairs);

        // DB
        DatabaseClient db = new Neo4jClient(uri, user, password, database, durationSeconds);
        db.connect();

        // Query pool (pre-created params/templates)
        QueryGenerator gen = new QueryGenerator(db);
        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries().forEach((k, v) -> qPool.put(k, new LinkedBlockingQueue<>(v)));

        // Worker pool: bounded fixed-size VT executor
        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                MAX_V_THREADS, MAX_V_THREADS, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_BACKLOG),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        long nowMs     = System.currentTimeMillis();
        long warmEnd   = nowMs + warmupSeconds * 1000L;
        long measureMs = durationSeconds - Math.max(0, warmupSeconds);
        long phaseEnd  = warmEnd + measureMs * 1000L;

        /* ---------- Warm-up (CLOSED) ---------- */
        if (warmupSeconds > 0) {
            launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, warmEnd, true);
            // Let warm-up run, then wait it out
            workers.awaitTermination(warmupSeconds + 1L, TimeUnit.SECONDS);
        }

        // We’ll track how many results existed at the start of the measured phase,
        // and at each interval boundary compute the “tail slice”.
        int prevSize;
        synchronized (results) { prevSize = results.size(); }

        /* ---------- Measured Phase ---------- */
        if (rampIntervalSec > 0) {
            // Ramping mode: interval-based scheduling + per-interval reports
            if (arrivalMode == ArrivalMode.CLOSED) {
                runClosedRamping(db, workers, qPool, warmEnd, phaseEnd,
                        rampIntervalSec, rampClients, maxClients, prevSize);
            } else {
                runOpenRamping(db, workers, qPool, warmEnd, phaseEnd,
                        rampIntervalSec, rampRate, maxRate, prevSize);
            }
        } else {
            // Non-ramping (single phase)
            if (arrivalMode == ArrivalMode.CLOSED) {
                launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, phaseEnd, false);
            } else {
                // single open-interval with current λ
                launchOpenSubmittersOneInterval(db, workers, qPool, phaseEnd, λ, /*label*/"Submitter");
            }
        }

        // Wind down & close
        workers.shutdown();
        workers.awaitTermination(5, TimeUnit.MINUTES);
        db.close();

        // ---------- Final report over measured phase ----------
        List<QueryResult> measured;
        synchronized (results) {
            measured = new ArrayList<>(results.subList(prevSize, results.size()));
        }

        // Workers meta for the FINAL line (best-effort):
        Map<String,Integer> finalWorkers = (arrivalMode == ArrivalMode.CLOSED)
                ? Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients)
                : Map.of("OLTP", 0, "GRAPH", 0, "OLAP", 0); // open mode doesn't have a stable "workers" count

        new MetricsAggregator(measured, (int) (measureMs), CSV_OUT, finalWorkers).printReport();

        Instant endTs = Instant.now();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(endTs.atZone(ZoneId.systemDefault()).toLocalTime()),
                human(Duration.between(startTs, endTs)));
        return 0;
    }

    /* ========================== CLOSED (non-ramping) ========================== */
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

    /* ============================ CLOSED (ramping) ============================ */
    private void runClosedRamping(DatabaseClient db,
                                  ThreadPoolExecutor pool,
                                  Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                  long warmEnd, long phaseEnd,
                                  int intervalSec,
                                  Map<String,Integer> rampClients, Map<String,Integer> maxClients,
                                  int initialPrevSize) {

        int prevSize = initialPrevSize;
        Map<String,Integer> current = new HashMap<>();
        current.put("OLTP", oltpClients);
        current.put("GRAPH", graphClients);
        current.put("OLAP", olapClients);

        int interval = 0;
        long cursor = warmEnd;
        while (cursor < phaseEnd) {
            interval++;
            long next = Math.min(phaseEnd, cursor + intervalSec * 1000L);
            int sec   = (int) ((next - cursor) / 1000L);
            if (sec <= 0) break;

            // Launch clients for this interval only
            launchClosedWorkers(db, pool,
                    current.getOrDefault("OLTP",0),
                    current.getOrDefault("GRAPH",0),
                    current.getOrDefault("OLAP",0),
                    qPool, next, /*warm*/ false);

            // Wait for this interval to pass
            sleepUntil(next);

            // ----- Per-interval report -----
            List<QueryResult> slice;
            synchronized (results) {
                slice = new ArrayList<>(results.subList(prevSize, results.size()));
                prevSize = results.size();
            }
            Map<String,Integer> workersMeta = Map.of(
                    "OLTP", current.getOrDefault("OLTP",0),
                    "GRAPH", current.getOrDefault("GRAPH",0),
                    "OLAP", current.getOrDefault("OLAP",0)
            );
            System.out.printf("%n=== Interval %d (CLOSED) — %d s ===%n", interval, sec);
            try {
                new MetricsAggregator(slice, sec, CSV_OUT, workersMeta).printReport();
            } catch (IOException e) {
                System.err.println("Interval CSV write failed: " + e.getMessage());
            }

            // ----- Ramp clients for next interval -----
            bumpInt(current, "OLTP", rampClients, maxClients);
            bumpInt(current, "GRAPH", rampClients, maxClients);
            bumpInt(current, "OLAP", rampClients, maxClients);

            cursor = next;
        }
    }

    /* ============================= OPEN (non-ramping) ============================= */
    private void launchOpenSubmittersOneInterval(DatabaseClient db,
                                                 ThreadPoolExecutor pool,
                                                 Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                                 long endMillis,
                                                 Map<String,Double> lambda,
                                                 String banner) {
        List<Thread> submitters = new ArrayList<>();
        startShardedSubmitters("OLTP", lambda.getOrDefault("OLTP", 0.0), pool, db, qPool.get("OLTP"),  endMillis, submitters, banner);
        startShardedSubmitters("GRAPH", lambda.getOrDefault("GRAPH", 0.0), pool, db, qPool.get("GRAPH"), endMillis, submitters, banner);
        startShardedSubmitters("OLAP",  lambda.getOrDefault("OLAP",  0.0), pool, db, qPool.get("OLAP"),  endMillis, submitters, banner);
        // join
        for (Thread t : submitters) { try { t.join(); } catch (InterruptedException ignore) {} }
    }

    /* =============================== OPEN (ramping) =============================== */
    private void runOpenRamping(DatabaseClient db,
                                ThreadPoolExecutor pool,
                                Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                long warmEnd, long phaseEnd,
                                int intervalSec,
                                Map<String,Double> rampRate, Map<String,Double> maxRate,
                                int initialPrevSize) {

        int prevSize = initialPrevSize;
        Map<String,Double> cur = new HashMap<>(λ); // current λ per cat

        int interval = 0;
        long cursor = warmEnd;
        while (cursor < phaseEnd) {
            interval++;
            long next = Math.min(phaseEnd, cursor + intervalSec * 1000L);
            int sec   = (int) ((next - cursor) / 1000L);
            if (sec <= 0) break;

            // Start submitters for *this* interval only
            List<Thread> submitters = new ArrayList<>();
            int shardsOLTP = startShardedSubmitters("OLTP", cur.getOrDefault("OLTP",0.0), pool, db, qPool.get("OLTP"),  next, submitters, "Submitter");
            int shardsGRAPH= startShardedSubmitters("GRAPH",cur.getOrDefault("GRAPH",0.0), pool, db, qPool.get("GRAPH"), next, submitters, "Submitter");
            int shardsOLAP = startShardedSubmitters("OLAP", cur.getOrDefault("OLAP", 0.0), pool, db, qPool.get("OLAP"),  next, submitters, "Submitter");

            // Wait interval to finish
            for (Thread t : submitters) { try { t.join(); } catch (InterruptedException ignore) {} }

            // ----- Per-interval report -----
            List<QueryResult> slice;
            synchronized (results) {
                slice = new ArrayList<>(results.subList(prevSize, results.size()));
                prevSize = results.size();
            }
            Map<String,Integer> workersMeta = Map.of(
                    "OLTP", shardsOLTP,
                    "GRAPH", shardsGRAPH,
                    "OLAP", shardsOLAP
            );
            System.out.printf("%n=== Interval %d (OPEN) — %d s ===%n", interval, sec);
            try {
                new MetricsAggregator(slice, sec, CSV_OUT, workersMeta).printReport();
            } catch (IOException e) {
                System.err.println("Interval CSV write failed: " + e.getMessage());
            }

            // ----- Ramp λ for next interval -----
            bumpDouble(cur, "OLTP", rampRate, maxRate);
            bumpDouble(cur, "GRAPH", rampRate, maxRate);
            bumpDouble(cur, "OLAP",  rampRate, maxRate);

            cursor = next;
        }
    }

    /* ---------------- helpers ---------------- */
    private static void sleepUntil(long epochMillis) {
        long now = System.currentTimeMillis();
        long wait = epochMillis - now;
        if (wait > 0) {
            try { Thread.sleep(wait); } catch (InterruptedException ignore) {}
        }
    }

    private int startShardedSubmitters(String cat, double lambda, ThreadPoolExecutor pool,
                                       DatabaseClient db, BlockingQueue<QueryTemplate.PreparedQuery> q,
                                       long endMs, List<Thread> out, String banner) {
        if (q == null || lambda <= 0.0) return 0;
        int shards = (int) Math.ceil(lambda / MAX_SUBMITTER_QPS);
        double per = lambda / shards;
        System.out.printf("%s %s will run at %.0f qps × %d shard(s)%n", banner, cat, per, shards);
        for (int i = 0; i < shards; i++) {
            out.add(Thread.startVirtualThread(
                    new ArrivalSubmitter(per, pool, db, q, cat, endMs, results)));
        }
        return shards;
    }

    private static void bumpDouble(Map<String,Double> cur, String key,
                                   Map<String,Double> step, Map<String,Double> cap) {
        double v = cur.getOrDefault(key, 0.0) + step.getOrDefault(key, 0.0);
        double max = cap.getOrDefault(key, Double.POSITIVE_INFINITY);
        cur.put(key, Math.min(v, max));
    }

    private static void bumpInt(Map<String,Integer> cur, String key,
                                Map<String,Integer> step, Map<String,Integer> cap) {
        int v = cur.getOrDefault(key, 0) + step.getOrDefault(key, 0);
        int max = cap.getOrDefault(key, Integer.MAX_VALUE);
        cur.put(key, Math.min(v, max));
    }

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

    private static String human(Duration d){
        return String.format("%d:%02d.%03d", d.toMinutes(), d.toSecondsPart(), d.toMillisPart());
    }
}
