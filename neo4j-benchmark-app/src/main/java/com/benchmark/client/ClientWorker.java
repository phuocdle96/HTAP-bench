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
    private final BlockingQueue<QueryTemplate.PreparedQuery> queryQueue;
    private final String category;
    private final long endTimeMillis;
    private final boolean isWarmup;

    public ClientWorker(DatabaseClient dbClient, BlockingQueue<QueryTemplate.PreparedQuery> queryQueue, String category, long endTimeMillis, boolean isWarmup) {
        this.dbClient = dbClient;
        this.queryQueue = queryQueue;
        this.category = category;
        this.endTimeMillis = endTimeMillis;
        this.isWarmup = isWarmup;
    }

    @Override
    public List<QueryResult> call() {
        List<QueryResult> results = new ArrayList<>();
        
        while (System.currentTimeMillis() < endTimeMillis) {
            QueryTemplate.PreparedQuery preparedQuery = null;
            try {
                preparedQuery = queryQueue.take();
                
                long startTime = System.nanoTime();
                dbClient.executeQuery(preparedQuery.cypher(), preparedQuery.params());
                long latencyNanos = System.nanoTime() - startTime;

                if (!isWarmup) {
                    results.add(new QueryResult(this.category, latencyNanos));
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
                    queryQueue.offer(preparedQuery);
                }
            }
        }
        return results;
    }
}