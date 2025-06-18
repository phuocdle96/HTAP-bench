package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class QueryGenerator {
    private final List<QueryTemplate> templates;
    private final Map<String, List<String>> sampleIds = new HashMap<>();

    public QueryGenerator(DatabaseClient dbClient) {
        this.templates = loadTemplates();
        fetchSampleIds(dbClient);
    }

    private List<QueryTemplate> loadTemplates() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("queries.json")) {
            if (is == null) throw new RuntimeException("Cannot find queries.json in resources.");
            InputStreamReader reader = new InputStreamReader(is);
            Type listType = new TypeToken<ArrayList<QueryTemplate>>() {}.getType();
            return new Gson().fromJson(reader, listType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load or parse query templates.", e);
        }
    }
    
    private void fetchSampleIds(DatabaseClient dbClient) {
        System.out.println("Fetching sample IDs for parameter generation...");
        sampleIds.put("patientId", dbClient.fetchSampleIds("MATCH (p:Patient) RETURN p.patientId as id LIMIT 10000", "id"));
        sampleIds.put("unitId", dbClient.fetchSampleIds("MATCH (h:HealthcareUnit) RETURN h.unitId as id LIMIT 10000", "id"));
        sampleIds.put("diagnosisCode", dbClient.fetchSampleIds("MATCH (d:Diagnosis) RETURN d.code as id LIMIT 10000", "id"));
        sampleIds.put("admissionId", dbClient.fetchSampleIds("MATCH (a:Admission) RETURN a.eventId as id LIMIT 10000", "id"));
        System.out.println("Sample IDs fetched.");
    }
    
    private Object getRandomParamValue(String paramName) {
        String key = paramName.toLowerCase();
        List<String> idList = sampleIds.get(key);

        if (idList != null && !idList.isEmpty()) {
            return idList.get(ThreadLocalRandom.current().nextInt(idList.size()));
        }
        
        return switch (key) {
            case "year" -> String.valueOf(ThreadLocalRandom.current().nextInt(2008, 2012));
            default -> UUID.randomUUID().toString(); // Default for new IDs
        };
    }

    public Map<String, List<QueryTemplate.PreparedQuery>> prepareAllQueries() {
        Map<String, List<QueryTemplate.PreparedQuery>> categorizedQueries = new HashMap<>();
        for (QueryTemplate template : templates) {
            categorizedQueries.computeIfAbsent(template.category(), k -> new ArrayList<>());

            int numVariations = "OLTP".equals(template.category()) ? 5000 : 500;
            
            for(int i = 0; i < numVariations; i++) {
                Map<String, Object> params = new HashMap<>();
                for (String paramName : template.params()) {
                    params.put(paramName, getRandomParamValue(paramName));
                }
                categorizedQueries.get(template.category()).add(new QueryTemplate.PreparedQuery(template.cypher(), params));
            }
        }
        return categorizedQueries;
    }
}