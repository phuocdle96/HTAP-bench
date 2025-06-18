package com.benchmark;

import com.benchmark.client.DatabaseClient;
import com.benchmark.client.Neo4jClient;
import com.benchmark.client.ClientWorker;
import com.benchmark.generator.QueryGenerator;
import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.MetricsAggregator;
import com.benchmark.metrics.QueryResult;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Command(name = "benchmark-runner", mixinStandardHelpOptions = true, version = "1.0",
        description = "Runs a scalable benchmark test against a Neo4j database.")
public class BenchmarkRunner implements Callable<Integer> {

    @Option(names = {"-u", "--uri"}, description = "Neo4j Bolt or Neo4j URI.", required = true)
    private String uri;

    @Option(names = {"--user"}, description = "Database user.", required = true)
    private String user;

    @Option(names = {"-p", "--password"}, description = "Database password.", required = true)
    private String password;
    
    @Option(names = {"--database"}, description = "Target database name.", defaultValue = "neo4j")
    private String database;

    // CORRECTED: Added separate client counts for each workload
    @Option(names = {"--oltp-clients"}, description = "Number of concurrent OLTP clients.", defaultValue = "8")
    private int oltpClientCount;

    @Option(names = {"--olap-clients"}, description = "Number of concurrent OLAP clients.", defaultValue = "4")
    private int olapClientCount;

    @Option(names = {"--graph-clients"}, description = "Number of concurrent GRAPH clients.", defaultValue = "4")
    private int graphClientCount;

    @Option(names = {"-d", "--duration"}, description = "Duration of the benchmark in seconds.", defaultValue = "60")
    private int durationSeconds;

    @Option(names = {"-w", "--warmup"}, description = "Warmup period in seconds.", defaultValue = "10")
    private int warmupSeconds;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new BenchmarkRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        System.out.println("--- Starting Neo4j Benchmark ---");
        System.out.printf("Configuration: URI=%s, Database=%s, OLTP Clients=%d, OLAP Clients=%d, GRAPH Clients=%d, Duration=%ds, Warmup=%ds%n",
                uri, database, oltpClientCount, olapClientCount, graphClientCount, durationSeconds, warmupSeconds);

        DatabaseClient dbClient = new Neo4jClient(uri, user, password, database);

        System.out.println("\n[PHASE 1] Initializing Query Generator...");
        QueryGenerator queryGenerator = new QueryGenerator(dbClient);
        Map<String, List<QueryTemplate.PreparedQuery>> preparedQueries = queryGenerator.prepareAllQueries();
        System.out.println("Query templates loaded and sample IDs fetched.");

        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> queryQueues = new ConcurrentHashMap<>();
        for(String category : preparedQueries.keySet()) {
            queryQueues.put(category, new LinkedBlockingQueue<>(preparedQueries.get(category)));
        }

        // Create separate thread pools for each workload
        ExecutorService oltpExecutor = Executors.newFixedThreadPool(oltpClientCount);
        ExecutorService olapExecutor = Executors.newFixedThreadPool(olapClientCount);
        ExecutorService graphExecutor = Executors.newFixedThreadPool(graphClientCount);
        
        List<QueryResult> allResults = Collections.synchronizedList(new ArrayList<>());
        
        System.out.printf("\n[PHASE 2] Starting warmup for %d seconds...%n", warmupSeconds);
        runWorkload(dbClient, oltpExecutor, oltpClientCount, queryQueues.get("OLTP"), "OLTP", warmupSeconds, true, null);
        runWorkload(dbClient, olapExecutor, olapClientCount, queryQueues.get("OLAP"), "OLAP", warmupSeconds, true, null);
        runWorkload(dbClient, graphExecutor, graphClientCount, queryQueues.get("GRAPH"), "GRAPH", warmupSeconds, true, null);
        System.out.println("Warmup complete.");


        System.out.printf("\n[PHASE 3] Starting benchmark for %d seconds...%n", durationSeconds);
        runWorkload(dbClient, oltpExecutor, oltpClientCount, queryQueues.get("OLTP"), "OLTP", durationSeconds, false, allResults);
        runWorkload(dbClient, olapExecutor, olapClientCount, queryQueues.get("OLAP"), "OLAP", durationSeconds, false, allResults);
        runWorkload(dbClient, graphExecutor, graphClientCount, queryQueues.get("GRAPH"), "GRAPH", durationSeconds, false, allResults);
        
        System.out.println("\n[PHASE 4] Benchmark finished. Shutting down executors...");
        shutdownExecutor(oltpExecutor);
        shutdownExecutor(olapExecutor);
        shutdownExecutor(graphExecutor);
        
        dbClient.close();
        
        System.out.println("Aggregating results...");
        MetricsAggregator aggregator = new MetricsAggregator(allResults, durationSeconds);
        aggregator.printReport();

        return 0;
    }

    private void runWorkload(DatabaseClient dbClient, ExecutorService executor, int clientCount, BlockingQueue<QueryTemplate.PreparedQuery> queue, String category, int duration, boolean isWarmup, List<QueryResult> results) throws InterruptedException, ExecutionException {
        if (clientCount <= 0 || queue == null) return;
        
        long endTime = System.currentTimeMillis() + (duration * 1000L);
        List<Future<List<QueryResult>>> futures = new ArrayList<>();

        for (int i = 0; i < clientCount; i++) {
            futures.add(executor.submit(new ClientWorker(dbClient, queue, category, endTime, isWarmup)));
        }

        for (Future<List<QueryResult>> future : futures) {
            List<QueryResult> workerResults = future.get();
            if (results != null) {
                results.addAll(workerResults);
            }
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}