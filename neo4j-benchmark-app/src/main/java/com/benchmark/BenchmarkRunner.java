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
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Neo4j HTAP benchmark – CLOSED (token) and OPEN (Poisson) arrival modes.
 * Uses virtual threads (JDK 21).
 */
@Command(name = "benchmark-runner",
         mixinStandardHelpOptions = true,
         description = "Neo4j HTAP benchmark – open / closed arrival modes")
public class BenchmarkRunner implements Callable<Integer> {

    /* ---------- CLI ---------- */
    @Option(names="--uri",      defaultValue="bolt://localhost:7687") String uri;
    @Option(names="--user",     defaultValue="neo4j")                 String user;
    @Option(names="--password", defaultValue="neo4j")                 String password;
    @Option(names="--database", defaultValue="neo4j")                 String database;

    @Option(names="--oltp-clients",  defaultValue="8") int oltpClients;
    @Option(names="--graph-clients", defaultValue="1") int graphClients;
    @Option(names="--olap-clients",  defaultValue="1") int olapClients;

    @Option(names={"-d","--duration"}, defaultValue="60") int durationSeconds;
    @Option(names={"-w","--warmup"},   defaultValue="10") int warmupSeconds;

    @Option(names="--arrival-mode", defaultValue="CLOSED") ArrivalMode arrivalMode;
    @Option(names="--arrival-rate", arity="1..*", split=",") List<String> λPairs = new ArrayList<>();

    /* ---------- state ---------- */
    private final Map<String,Double> λ = new HashMap<>();
    private final List<QueryResult>  results =
            Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    /* ================================================================ */
    @Override public Integer call() throws Exception {

        Instant start = Instant.now();
        System.out.printf("Benchmark started at %s%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(
                        start.atZone(java.time.ZoneId.systemDefault()).toLocalTime()));

        λPairs.stream()
              .map(s->s.split("="))
              .filter(a->a.length==2)
              .forEach(a->λ.put(a[0].toUpperCase(Locale.ROOT),
                                Double.parseDouble(a[1])));

        DatabaseClient db = new Neo4jClient(uri,user,password,database,durationSeconds);
        db.connect();

        QueryGenerator gen = new QueryGenerator(db);
        Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> qPool = new HashMap<>();
        gen.prepareAllQueries()
           .forEach((k,v)->qPool.put(k,new LinkedBlockingQueue<>(v)));

        /* ---------- warm-up ---------- */
        runClosed(db, warmupSeconds,
                  oltpClients,graphClients,olapClients,
                  qPool,true);

        /* ---------- measured ---------- */
        if (arrivalMode==ArrivalMode.CLOSED)
            runClosed(db,durationSeconds-warmupSeconds,
                      oltpClients,graphClients,olapClients,
                      qPool,false);
        else
            runOpen(db,durationSeconds-warmupSeconds,qPool);

        /* ---------- report ---------- */
        new MetricsAggregator(results,
                              durationSeconds-warmupSeconds,
                              "benchmark_results.csv",
                              Map.of("OLTP",oltpClients,
                                     "GRAPH",graphClients,
                                     "OLAP",olapClients))
            .printReport();

        db.close();
        System.out.printf("Benchmark finished at %s (elapsed %s)%n",
                DateTimeFormatter.ISO_LOCAL_TIME.format(
                        Instant.now()
                               .atZone(java.time.ZoneId.systemDefault())
                               .toLocalTime()),
                human(Duration.between(start,Instant.now())));
        return 0;
    }

    /* ================================================================ */
    private void runClosed(DatabaseClient db,int sec,
                           int oltp,int graph,int olap,
                           Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> qPool,
                           boolean warm) throws InterruptedException {

        ExecutorService oltpPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        ExecutorService graphPool= Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        ExecutorService olapPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        long end = System.currentTimeMillis()+sec*1000L;

        launch(db,oltpPool,oltp,qPool.get("OLTP"),"OLTP",end,warm,false);
        launch(db,graphPool,graph,qPool.get("GRAPH"),"GRAPH",end,warm,false);
        launch(db,olapPool, olap,qPool.get("OLAP"), "OLAP", end,warm,false);

        oltpPool.shutdown(); graphPool.shutdown(); olapPool.shutdown();
        oltpPool.awaitTermination(5,TimeUnit.MINUTES);
        graphPool.awaitTermination(5,TimeUnit.MINUTES);
        olapPool.awaitTermination(5,TimeUnit.MINUTES);
    }

    /* ---------- OPEN mode with bounded worker pool ---------- */
    private void runOpen(DatabaseClient db,int sec,
                         Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> qPool)
            throws InterruptedException {

        int maxVThreads=512;                                      // cap
        ExecutorService workerPool=
                Executors.newFixedThreadPool(maxVThreads,Thread.ofVirtual().factory());

        long endMs=System.currentTimeMillis()+sec*1000L;

        /* Start each submitter on its own virtual thread */
        List<Thread> submitters=new ArrayList<>();

        if (λ.getOrDefault("OLTP",0.0)>0)
            submitters.add(Thread.startVirtualThread(
                new ArrivalSubmitter(λ.get("OLTP"),workerPool,db,
                                     qPool.get("OLTP"),"OLTP",endMs,results)));

        if (λ.getOrDefault("GRAPH",0.0)>0)
            submitters.add(Thread.startVirtualThread(
                new ArrivalSubmitter(λ.get("GRAPH"),workerPool,db,
                                     qPool.get("GRAPH"),"GRAPH",endMs,results)));

        if (λ.getOrDefault("OLAP",0.0)>0)
            submitters.add(Thread.startVirtualThread(
                new ArrivalSubmitter(λ.get("OLAP"),workerPool,db,
                                     qPool.get("OLAP"),"OLAP",endMs,results)));

        /* Wait for all arrival threads to finish */
        for (Thread t:submitters) t.join();

        /* Now it's safe to shut the worker pool */
        workerPool.shutdown();
        workerPool.awaitTermination(5,TimeUnit.MINUTES);
    }

    /* ---------- helpers ---------- */
    private void launch(DatabaseClient db,ExecutorService pool,int n,
                        BlockingQueue<QueryTemplate.PreparedQuery> q,
                        String cat,long end,boolean warm,boolean singleShot){
        for(int i=0;i<n;i++)
            pool.submit(new ClientWorker(db,q,cat,end,warm,singleShot,results));
    }
    private static String human(Duration d){
        return String.format("%d:%02d.%03d",d.toMinutes(),d.toSecondsPart(),d.toMillisPart());
    }
}
