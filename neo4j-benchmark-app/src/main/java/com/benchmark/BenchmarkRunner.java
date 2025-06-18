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

    @Option(names = {"-c", "--clients"}, description = "Number of concurrent clients.", defaultValue = "10")
    private int clientCount;

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
        System.out.printf("Configuration: URI=%s, Database=%s, Clients=%d, Duration=%ds, Warmup=%ds%n",
                uri, database, clientCount, durationSeconds, warmupSeconds);

        DatabaseClient dbClient = new Neo4jClient(uri, user, password, database);

        System.out.println("\n[PHASE 1] Initializing Query Generator...");
        QueryGenerator queryGenerator = new QueryGenerator(dbClient);
        Map<String, List<QueryTemplate.PreparedQuery>> preparedQueries = queryGenerator.prepareAllQueries();
        System.out.println("Query templates loaded and sample IDs fetched.");

        Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> queryQueues = new ConcurrentHashMap<>();
        for(String category : preparedQueries.keySet()) {
            queryQueues.put(category, new LinkedBlockingQueue<>(preparedQueries.get(category)));
        }

        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        List<QueryResult> allResults = Collections.synchronizedList(new ArrayList<>());

        System.out.printf("\n[PHASE 2] Starting %d clients for a %d second warmup...%n", clientCount, warmupSeconds);
        long warmupEndTime = System.currentTimeMillis() + (warmupSeconds * 1000L);
        List<Future<List<QueryResult>>> warmupFutures = new ArrayList<>();
        for (int i = 0; i < clientCount; i++) {
            warmupFutures.add(executor.submit(new ClientWorker(dbClient, queryQueues, warmupEndTime, true)));
        }
        for(Future<List<QueryResult>> future : warmupFutures) {
            future.get(); // Wait for warmup to complete
        }
        System.out.println("Warmup complete.");


        System.out.printf("\n[PHASE 3] Starting %d clients for a %d second benchmark run...%n", clientCount, durationSeconds);
        long benchmarkEndTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        List<Future<List<QueryResult>>> benchmarkFutures = new ArrayList<>();
        for (int i = 0; i < clientCount; i++) {
            benchmarkFutures.add(executor.submit(new ClientWorker(dbClient, queryQueues, benchmarkEndTime, false)));
        }

        for (Future<List<QueryResult>> future : benchmarkFutures) {
            allResults.addAll(future.get());
        }

        System.out.println("\n[PHASE 4] Benchmark finished. Aggregating results...");
        executor.shutdown();
        dbClient.close();

        MetricsAggregator aggregator = new MetricsAggregator(allResults, durationSeconds);
        aggregator.printReport();

        return 0;
    }
}