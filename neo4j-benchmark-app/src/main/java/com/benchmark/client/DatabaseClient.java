package com.benchmark.client;

import java.util.List;
import java.util.Map;

public interface DatabaseClient extends AutoCloseable {
    void connect();
    List<Map<String, Object>> executeQuery(String query, Map<String, Object> params);
    List<String> fetchSampleIds(String query, String idColumn);
    @Override
    void close();
}