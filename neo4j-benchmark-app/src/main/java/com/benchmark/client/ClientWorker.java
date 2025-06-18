package com.benchmark.client;

import com.benchmark.generator.QueryTemplate;
import com.benchmark.metrics.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class ClientWorker implements Callable<List<QueryResult>> {
    private final DatabaseClient dbClient;
    private final Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> queryQueues;
    private final long endTimeMillis;
    private final boolean isWarmup;

    public ClientWorker(DatabaseClient dbClient, Map<String, BlockingQueue<QueryTemplate.PreparedQuery>> queryQueues, long endTimeMillis, boolean isWarmup) {
        this.dbClient = dbClient;
        this.queryQueues = queryQueues;
        this.endTimeMillis = endTimeMillis;
        this.isWarmup = isWarmup;
    }

    @Override
    public List<QueryResult> call() {
        List<QueryResult> results = new ArrayList<>();
        String[] categories = queryQueues.keySet().toArray(new String[0]);
        int categoryIndex = 0;

        while (System.currentTimeMillis() < endTimeMillis) {
            String category = categories[categoryIndex % categories.length];
            categoryIndex++;
            
            BlockingQueue<QueryTemplate.PreparedQuery> queue = queryQueues.get(category);
            QueryTemplate.PreparedQuery preparedQuery = null;

            try {
                preparedQuery = queue.take();
                
                long startTime = System.nanoTime();
                dbClient.executeQuery(preparedQuery.cypher(), preparedQuery.params());
                long latencyNanos = System.nanoTime() - startTime;

                if (!isWarmup) {
                    results.add(new QueryResult(category, latencyNanos));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!isWarmup) {
                     System.err.printf("Client failed to execute query: %s. Error: %s%n", preparedQuery != null ? preparedQuery.cypher() : "N/A", e.getMessage());
                }
            } finally {
                if (preparedQuery != null) {
                    queue.offer(preparedQuery);
                }
            }
        }
        return results;
    }
}