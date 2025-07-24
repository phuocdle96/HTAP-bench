// neo4j-benchmark-app/src/main/java/com/benchmark/BenchmarkRunner.java
// ------------------------------------------------------------------
// Updated 2025‑07‑17 (fix build):
//   • Add missing imports (picocli.CommandLine, DateTimeFormatter).
//   • Restore thread‑safe `results` list; pass it to ClientWorker & ArrivalSubmitter.
//   • Use correct constructor arity (singleShot=false).
//   • Remove invalid db.results() call – MetricsAggregator gets the shared list.

package com.benchmark;

import com.benchmark.arrival.ArrivalMode;
import com.benchmark.arrival.ArrivalSubmitter;
import com.benchmark.client.*;
import com.benchmark.generator.*;
import com.benchmark.metrics.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

@Command(name="benchmark-runner", mixinStandardHelpOptions = true,
         description = "Neo4j HTAP benchmark – open / closed arrival modes")
public class BenchmarkRunner implements Callable<Integer> {

    /* ---------------- CLI flags ---------------- */
    @Option(names = "--uri",      defaultValue = "bolt://localhost:7687") String uri;
    @Option(names = "--user",     defaultValue = "neo4j")                 String user;
    @Option(names = "--password", defaultValue = "neo4j")                 String password;
    @Option(names = "--database", defaultValue = "neo4j")                 String database;

    // CLOSED‑loop worker counts
    @Option(names = "--oltp-clients",  defaultValue = "8") int oltpClients;
    @Option(names = "--graph-clients", defaultValue = "1") int graphClients;
    @Option(names = "--olap-clients",  defaultValue = "1") int olapClients;

    // OPEN‑loop arrival rates (QPS)
    @Option(names = "--arrival-mode", defaultValue = "CLOSED") ArrivalMode arrivalMode;
    @Option(names = "--arrival-rate", arity = "1..*", split = ",") List<String> arrivalPairs = new ArrayList<>();

    @Option(names = {"-d", "--duration"}, defaultValue = "180") int durationSeconds;
    @Option(names = {"-w", "--warmup"},   defaultValue = "10")  int warmupSeconds;

    /* ---------------- constants ---------------- */
    private static final int MAX_V_THREADS     = 512;
    private static final int MAX_BACKLOG       = 20_000;
    private static final int MAX_SUBMITTER_QPS = 300;   // per shard

    /* ---------------- internal ---------------- */
    private final Map<String, Double> λ = new HashMap<>();
    private final List<QueryResult>    results = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    /* ================================================================ */
    @Override
    public Integer call() throws Exception {
        Instant startTs = Instant.now();
        System.out.printf("Benchmark started at %s%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(startTs.atZone(ZoneId.systemDefault()).toLocalTime()));

        /* ---------- parse λ from CLI ---------- */
        arrivalPairs.stream()
                .map(s -> s.split("="))
                .filter(a -> a.length == 2)
                .forEach(a -> λ.put(a[0].toUpperCase(Locale.ROOT), Double.parseDouble(a[1])));

        /* ---------- connect DB ---------- */
        DatabaseClient db = new Neo4jClient(uri, user, password, database, durationSeconds);
        db.connect();

        /* ---------- prepare queries ---------- */
        QueryGenerator gen = new QueryGenerator(db);
        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries().forEach((k, v) -> qPool.put(k, new LinkedBlockingQueue<>(v)));

        /* ---------- worker pool (bounded) ---------- */
        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                MAX_V_THREADS, MAX_V_THREADS, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_BACKLOG),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        long warmEnd  = System.currentTimeMillis() + warmupSeconds * 1000L;
        long phaseEnd = warmEnd + (durationSeconds - warmupSeconds) * 1000L;

        /* ---------- Phase A – warm‑up ---------- */
        launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, warmEnd, true);
        workers.awaitTermination(warmupSeconds + 1L, TimeUnit.SECONDS);

        /* ---------- Phase B ---------- */
        if (arrivalMode == ArrivalMode.CLOSED) {
            launchClosedWorkers(db, workers, oltpClients, graphClients, olapClients, qPool, phaseEnd, false);
        } else {
            launchOpenSubmitters(db, workers, qPool, phaseEnd);
        }

        workers.shutdown();
        workers.awaitTermination(5, TimeUnit.MINUTES);
        db.close();

        new MetricsAggregator(results, durationSeconds - warmupSeconds, "benchmark_results.csv",
                Map.of("OLTP", oltpClients, "GRAPH", graphClients, "OLAP", olapClients))
                .printReport();

        Instant endTs = Instant.now();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(endTs.atZone(ZoneId.systemDefault()).toLocalTime()),
                Duration.between(startTs, endTs));
        return 0;
    }

    /* ================================================================ */
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
        for (int i = 0; i < nThreads; i++)
            pool.submit(new ClientWorker(db, q, cat, endMillis, warm, /*singleShot*/ false, results));
    }

    /* ---------------- OPEN mode ---------------- */
    private void launchOpenSubmitters(DatabaseClient db, ThreadPoolExecutor pool,
                                      Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                                      long phaseEnd) {
        /* start submitters and collect threads */
        List<Thread> submitters = new ArrayList<>();
        submitters.addAll(startShardedSubmitters("OLTP", λ.getOrDefault("OLTP", 0.0), pool, db, qPool.get("OLTP"),  phaseEnd));
        submitters.addAll(startShardedSubmitters("GRAPH", λ.getOrDefault("GRAPH", 0.0), pool, db, qPool.get("GRAPH"), phaseEnd));
        submitters.addAll(startShardedSubmitters("OLAP", λ.getOrDefault("OLAP", 0.0), pool, db, qPool.get("OLAP"),  phaseEnd));

        // wait for all arrival threads to finish their phase
        for (Thread t : submitters) {
            try { t.join(); } catch (InterruptedException ignore) {}
        }
    }

    private List<Thread> startShardedSubmitters(String cat, double lambda, ThreadPoolExecutor pool,
                                        DatabaseClient db, BlockingQueue<QueryTemplate.PreparedQuery> q,
                                        long phaseEnd) {
        List<Thread> threads = new ArrayList<>();
        if (lambda <= 0.0) return threads;
        int shards = (int) Math.ceil(lambda / MAX_SUBMITTER_QPS);
        double λPerShard = lambda / shards;
        System.out.printf("Submitter %s will run at %.0f qps × %d shard(s)%n", cat, λPerShard, shards);
        for (int i = 0; i < shards; i++) {
            Thread t = Thread.startVirtualThread(new ArrivalSubmitter(λPerShard, pool, db, q, cat, phaseEnd, results));
            threads.add(t);
        }
        return threads;
    }
}
