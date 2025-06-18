package com.benchmark.generator;

import java.util.List;
import java.util.Map;

public record QueryTemplate(String name, String category, List<String> params, String cypher) {
    public record PreparedQuery(String cypher, Map<String, Object> params) {}
}