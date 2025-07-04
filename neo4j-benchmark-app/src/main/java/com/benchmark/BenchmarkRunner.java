package com.benchmark;

import com.benchmark.arrival.ArrivalMode;
import com.benchmark.arrival.ArrivalTimer;
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
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

@Command(name="benchmark-runner", mixinStandardHelpOptions=true,
         description="Neo4j HTAP benchmark – open / closed arrival modes")
public class BenchmarkRunner implements Callable<Integer> {

    /* ---------------- CLI flags ---------------- */
    @Option(names="--uri",     defaultValue="bolt://localhost:7687") String uri;
    @Option(names="--user",    defaultValue="neo4j")                 String user;
    @Option(names="--password",defaultValue="neo4j")                 String password;
    @Option(names="--database",defaultValue="neo4j")                 String database;

    @Option(names="--oltp-clients",  defaultValue="8")  int oltpClients;
    @Option(names="--graph-clients", defaultValue="1")  int graphClients;
    @Option(names="--olap-clients",  defaultValue="1")  int olapClients;

    @Option(names={"-d","--duration"}, defaultValue="60") int durationSeconds;
    @Option(names={"-w","--warmup"},   defaultValue="10") int warmupSeconds;

    @Option(names="--arrival-mode", defaultValue="CLOSED") ArrivalMode arrivalMode;
    @Option(names="--arrival-rate", arity="1..*", split=",") List<String> arrivalRatePairs = new ArrayList<>();

    /* ---------------- constants ---------------- */
    private static final int    SHARDS              = 10;      // # ArrivalTimer shards
    private static final double SHARD_THRESHOLD_QPS = 1_000.0; // shard when λ ≥ this

    /* ---------------- internal ---------------- */
    private final Map<String,Double> λ = new HashMap<>();
    private final List<Future<List<QueryResult>>> futures = new ArrayList<>();

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    /* ================================================================ */
    @Override public Integer call() throws Exception {

        Instant startTs = Instant.now();
        System.out.printf("Benchmark started at %s%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(
                        startTs.atZone(java.time.ZoneId.systemDefault()).toLocalTime()));

        /* ---------- parse λ from CLI ---------- */
        arrivalRatePairs.stream()
            .map(s->s.split("="))
            .filter(a->a.length==2)
            .forEach(a->λ.put(a[0].toUpperCase(Locale.ROOT),
                              Double.parseDouble(a[1])));

        /* ---------- connect DB ---------- */
        DatabaseClient db = new Neo4jClient(uri,user,password,database,durationSeconds);
        db.connect();

        /* ---------- prepare queries ---------- */
        QueryGenerator gen = new QueryGenerator(db);
        Map<String,List<QueryTemplate.PreparedQuery>> prep = gen.prepareAllQueries();
        Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> qMap = new HashMap<>();
        prep.forEach((k,v)->qMap.put(k,new LinkedBlockingQueue<>(v)));

        /* ---------- token queues (unbounded) ---------- */
        BlockingQueue<Object> oltpTok  = new LinkedBlockingQueue<>();
        BlockingQueue<Object> graphTok = new LinkedBlockingQueue<>();
        BlockingQueue<Object> olapTok  = new LinkedBlockingQueue<>();

        /* ---------- ArrivalTimer shards (OPEN) ---------- */
        long phaseBEnd = System.currentTimeMillis() + durationSeconds*1000L;
        ExecutorService timers    = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        if (arrivalMode == ArrivalMode.OPEN) {
            startShardedTimers("OLTP",  λ.getOrDefault("OLTP",0.0),  oltpTok,  phaseBEnd, timers);
            startShardedTimers("GRAPH", λ.getOrDefault("GRAPH",0.0), graphTok, phaseBEnd, timers);
            startShardedTimers("OLAP",  λ.getOrDefault("OLAP",0.0),  olapTok,  phaseBEnd, timers);
        }

        /* ---------- Phase A – warm-up ---------- */
        runPhase(db, warmupSeconds,
                 oltpClients, graphClients, olapClients,
                 qMap, oltpTok, graphTok, olapTok,
                 true);

        /* ---------- Phase B – measured ---------- */
        List<QueryResult> results = runPhase(db, durationSeconds - warmupSeconds,
                 oltpClients, graphClients, olapClients,
                 qMap, oltpTok, graphTok, olapTok,
                 false);

        /* ---------- cleanup ---------- */
        timers.shutdownNow();
        db.close();

        new MetricsAggregator(results, durationSeconds - warmupSeconds).printReport();

        Instant endTs = Instant.now();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(
                        endTs.atZone(java.time.ZoneId.systemDefault()).toLocalTime()),
                human(Duration.between(startTs,endTs)));

        return 0;
    }

    /* ================================================================ */
    private void startShardedTimers(String cat,
                                    double lambda,
                                    BlockingQueue<Object> tokQ,
                                    long endMillis,
                                    ExecutorService timers) {

        if (lambda <= 0.0) return;

        int shards = (lambda < SHARD_THRESHOLD_QPS) ? 1 : SHARDS;
        double λPerShard = lambda / shards;

        for (int i=0;i<shards;i++)
            timers.execute(new ArrivalTimer(λPerShard, tokQ, endMillis));

        System.out.printf("Started %d ArrivalTimer shard(s) for %s at %.0f qps each%n",
                          shards, cat, λPerShard);
    }

    /* ---------- run a phase ---------- */
    private List<QueryResult> runPhase(DatabaseClient db,
                                       int sec,
                                       int oltpThr,int graphThr,int olapThr,
                                       Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> qMap,
                                       BlockingQueue<Object> oltpTok,
                                       BlockingQueue<Object> graphTok,
                                       BlockingQueue<Object> olapTok,
                                       boolean warm) throws InterruptedException, ExecutionException {

        ExecutorService oltpPool  = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        ExecutorService graphPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        ExecutorService olapPool  = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        long end = System.currentTimeMillis() + sec*1000L;

        launchWorkers(db, oltpPool,  oltpThr,  qMap.get("OLTP"),
                      arrivalMode==ArrivalMode.OPEN ? oltpTok  : null,
                      "OLTP", end, warm);
        launchWorkers(db, graphPool, graphThr, qMap.get("GRAPH"),
                      arrivalMode==ArrivalMode.OPEN ? graphTok : null,
                      "GRAPH", end, warm);
        launchWorkers(db, olapPool,  olapThr,  qMap.get("OLAP"),
                      arrivalMode==ArrivalMode.OPEN ? olapTok  : null,
                      "OLAP", end, warm);

        oltpPool.shutdown(); graphPool.shutdown(); olapPool.shutdown();
        oltpPool.awaitTermination(5, TimeUnit.MINUTES);
        graphPool.awaitTermination(5, TimeUnit.MINUTES);
        olapPool.awaitTermination(5, TimeUnit.MINUTES);

        List<QueryResult> out = new ArrayList<>();
        for (Future<List<QueryResult>> f : futures) {
            if (!warm) out.addAll(f.get()); else f.get();
        }
        futures.clear();
        return out;
    }

    /* ---------- submit workers ---------- */
    private void launchWorkers(DatabaseClient db,
                               ExecutorService pool,
                               int nThreads,
                               BlockingQueue<QueryTemplate.PreparedQuery> q,
                               BlockingQueue<Object> tokQ,
                               String cat,
                               long endMillis,
                               boolean warm) {
        for (int i=0;i<nThreads;i++)
            futures.add(pool.submit(new ClientWorker(db,q,tokQ,cat,endMillis,warm)));
    }

    /* ---------- pretty duration ---------- */
    private static String human(Duration d){
        long s = d.getSeconds();
        return String.format("%d:%02d.%03d", s/60, s%60, d.toMillisPart());
    }
}
