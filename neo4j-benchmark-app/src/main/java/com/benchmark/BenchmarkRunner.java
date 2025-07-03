package com.benchmark;

import com.benchmark.arrival.ArrivalMode;
import com.benchmark.arrival.ArrivalScheduler;
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
import picocli.CommandLine.ITypeConverter;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Command(name = "benchmark-runner", mixinStandardHelpOptions = true,
         description = "Neo4j HTAP benchmark")
public class BenchmarkRunner implements Callable<Integer> {

    /* --- enum converter (case-insensitive) --- */
    static class CaseInsensitiveEnumConverter implements ITypeConverter<ArrivalMode> {
        @Override public ArrivalMode convert(String v) {
            return ArrivalMode.valueOf(v.toUpperCase(Locale.ROOT));
        }
    }

    /* --- CLI flags --- */
    @Option(names="--uri",     defaultValue="neo4j://localhost:7687") String uri;
    @Option(names="--user",    defaultValue="neo4j")                 String user;
    @Option(names="--password",defaultValue="neo4j")                 String password;
    @Option(names="--database",defaultValue="neo4j")                 String database;

    @Option(names="--oltp-clients", defaultValue="8")  int oltpClients;
    @Option(names="--graph-clients",defaultValue="1")  int graphClients;
    @Option(names="--olap-clients", defaultValue="1")  int olapClients;

    @Option(names={"-d","--duration"}, defaultValue="60") int durationSeconds;
    @Option(names={"-w","--warmup"},   defaultValue="30") int warmupSeconds;

    @Option(names="--arrival-mode", defaultValue="CLOSED",
            converter=CaseInsensitiveEnumConverter.class)
    ArrivalMode arrivalMode;

    @Option(names="--arrival-rate", arity="1..*", split=",",
            description="e.g. OLTP=5000") List<String> arrivalRatePairs = new ArrayList<>();

    private final Map<String,Double> arrivalRates = new HashMap<>();

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkRunner()).execute(args));
    }

    @Override public Integer call() throws Exception {

        arrivalRatePairs.stream()
            .map(p -> p.split("="))
            .filter(p -> p.length==2)
            .forEach(p -> arrivalRates.put(p[0].toUpperCase(Locale.ROOT),
                                           Double.parseDouble(p[1])));

        DatabaseClient db = new Neo4jClient(uri, user, password, database,
                                          durationSeconds);
        db.connect();

        QueryGenerator gen = new QueryGenerator(db);
        Map<String,List<QueryTemplate.PreparedQuery>> prepared = gen.prepareAllQueries();
        Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> queues = new ConcurrentHashMap<>();
        prepared.forEach((k,v) -> queues.put(k,new LinkedBlockingQueue<>(v)));

        ExecutorService oltpPool  = Executors.newFixedThreadPool(oltpClients);
        ExecutorService graphPool = Executors.newFixedThreadPool(graphClients);
        ExecutorService olapPool  = Executors.newFixedThreadPool(olapClients);

        List<QueryResult> results = Collections.synchronizedList(new ArrayList<>());

        runPhase(db,queues,oltpPool,graphPool,olapPool,warmupSeconds,true, results);
        runPhase(db,queues,oltpPool,graphPool,olapPool,
                 durationSeconds-warmupSeconds,false,results);

        oltpPool.shutdown();
        graphPool.shutdown();
        olapPool.shutdown();

        oltpPool.awaitTermination(5, TimeUnit.MINUTES);
        graphPool.awaitTermination(5, TimeUnit.MINUTES);
        olapPool.awaitTermination(5, TimeUnit.MINUTES);
        new MetricsAggregator(results,durationSeconds-warmupSeconds).printReport();
        db.close();
        return 0;
    }

    /* -------- run OLTP + GRAPH + OLAP concurrently -------- */
    private void runPhase(DatabaseClient db,
                          Map<String,BlockingQueue<QueryTemplate.PreparedQuery>> q,
                          ExecutorService oltp, ExecutorService graph, ExecutorService olap,
                          int durSec, boolean warm, List<QueryResult> sink)
            throws InterruptedException {

        ExecutorService phase = Executors.newFixedThreadPool(3);

        phase.execute(() -> {
            try {
                runWork(db, oltp, oltpClients, q.get("OLTP"),
                        "OLTP", durSec, warm, sink);
            } catch (Exception e) { e.printStackTrace(); }
        });

        phase.execute(() -> {
            try {
                runWork(db, graph, graphClients, q.get("GRAPH"),
                        "GRAPH", durSec, warm, sink);
            } catch (Exception e) { e.printStackTrace(); }
        });

        phase.execute(() -> {
            try {
                runWork(db, olap, olapClients, q.get("OLAP"),
                        "OLAP", durSec, warm, sink);
            } catch (Exception e) { e.printStackTrace(); }
        });

        phase.shutdown();
        phase.awaitTermination(durSec + 5L, TimeUnit.SECONDS);
    }

    /* -------- per-category workload -------- */
    private void runWork(DatabaseClient db, ExecutorService pool, int nThreads,
                         BlockingQueue<QueryTemplate.PreparedQuery> queue,
                         String cat, int durSec, boolean warm,
                         List<QueryResult> sink)
            throws InterruptedException, ExecutionException {

        if (nThreads<=0 || queue==null) return;
        long end = System.currentTimeMillis()+durSec*1000L;
        double λcat = arrivalMode==ArrivalMode.OPEN ? arrivalRates.getOrDefault(cat,0.0):0.0;

        List<Future<List<QueryResult>>> fut = new ArrayList<>();
        for(int i=0;i<nThreads;i++){
            ArrivalScheduler sch = λcat>0 ? new ArrivalScheduler(λcat/nThreads) : null;
            fut.add(pool.submit(new ClientWorker(db,queue,cat,end,warm,sch)));
        }
        for(Future<List<QueryResult>> f : fut){
            if(!warm && sink!=null) sink.addAll(f.get()); else f.get();
        }
    }
}
