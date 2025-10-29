package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.benchmark.generator.QueryLanguage.CYPHER;
import static com.benchmark.generator.QueryLanguage.GREMLIN;

/**
 * QueryGenerator that loads templates from classpath resource: src/main/resources/queries.json
 * and binds parameters using sampled IDs from the database (with synthetic fallbacks).
 *
 * Engine selection:
 *  - NEO4J / MEMGRAPH -> uses template.cypher
 *  - JANUSGRAPH       -> uses template.gremlin
 */
public class QueryGenerator {

    /** Pick which query dialects/templates to emit. */
    public enum Engine { NEO4J, MEMGRAPH, JANUSGRAPH }

    private final DatabaseClient db;
    private final Engine engine;

    /* Sample pools for param binding */
    private List<String> patientIds = List.of();
    private List<String> unitIds    = List.of();
    private List<String> diagCodes  = List.of();
    private List<String> years      = List.of("2021","2022","2023","2024","2025");

    public QueryGenerator(DatabaseClient db, Engine engine) {
        this.db = db;
        this.engine = engine;
        loadSamples();
    }

    /** Build query pools per category from queries.json */
    public Map<String, List<QueryTemplate.PreparedQuery>> prepareAllQueries() {
        List<Template> templates = readTemplatesFromClasspath();
        Map<String, List<QueryTemplate.PreparedQuery>> out = new HashMap<>();
        out.put("OLTP",  new ArrayList<>());
        out.put("GRAPH", new ArrayList<>());
        out.put("OLAP",  new ArrayList<>());

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (Template t : templates) {
            String cat = (t.category == null) ? "OLTP" : t.category.toUpperCase(Locale.ROOT);
            String text = chooseTextForEngine(t);
            if (text == null || text.isBlank()) {
                // No query for this engine in this template; skip it
                continue;
            }

            int copies = switch (cat) {
                case "OLTP"  -> 10_000;
                case "GRAPH" ->  2_000;
                case "OLAP"  ->    500;
                default      ->    500;
            };

            for (int i = 0; i < copies; i++) {
                Map<String,Object> params = bindParams(t.params, rnd);
                QueryLanguage lang = (engine == Engine.JANUSGRAPH) ? GREMLIN : CYPHER;
                out.get(cat).add(new QueryTemplate.PreparedQuery(lang, text, params));
            }
        }
        return out;
    }

    /* ============================ internals ============================ */

    private String chooseTextForEngine(Template t) {
        return switch (engine) {
            case NEO4J, MEMGRAPH -> t.cypher;
            case JANUSGRAPH      -> t.gremlin;
        };
    }

    /** Bind well-known parameters (fallback to synthetic if samples are empty). */
    private Map<String,Object> bindParams(List<String> names, ThreadLocalRandom rnd) {
        if (names == null || names.isEmpty()) return Map.of();
        Map<String,Object> p = new HashMap<>();
        for (String n : names) {
            switch (n) {
                case "patientId"     -> p.put("patientId", pick(patientIds, rnd));
                case "unitId"        -> p.put("unitId", pick(unitIds, rnd));
                case "diagnosisCode" -> p.put("diagnosisCode", pick(diagCodes, rnd));
                case "admissionId"   -> p.put("admissionId", UUID.randomUUID().toString());
                case "admissionDate" -> {
                    String y = pick(years, rnd);
                    String d = "%s-%02d-%02d".formatted(y, rnd.nextInt(1,13), rnd.nextInt(1,29));
                    p.put("admissionDate", d);
                    // Some Gremlin templates may also rely on epoch-days; add if helpful:
                    // long epochDays = LocalDate.parse(d).toEpochDay();
                    // p.put("admissionDateEpochDays", epochDays);
                }
                case "year"          -> p.put("year", pick(years, rnd));
                default              -> p.put(n, "X" + rnd.nextInt(1_000_000));
            }
        }
        return p;
    }

    private static <T> T pick(List<T> list, ThreadLocalRandom rnd) {
        return list.get(rnd.nextInt(list.size()));
    }

    /** Load ID samples from the DB; fallback to synthetic if empty */
    private void loadSamples() {
        try {
            patientIds = db.fetchSampleIds("Patient",        "patientId");
            unitIds    = db.fetchSampleIds("HealthcareUnit", "unitId");
            diagCodes  = db.fetchSampleIds("Diagnosis", "code");
        } catch (Exception ignore) { /* fall back below */ }

        if (patientIds == null || patientIds.isEmpty()) patientIds = synthetic("P%08d",  50_000);
        if (unitIds    == null || unitIds.isEmpty())    unitIds    = synthetic("U%05d",   1_000);
        if (diagCodes  == null || diagCodes.isEmpty())  diagCodes  = synthetic("D%05d",   5_000);
    }

    private static List<String> synthetic(String fmt, int n) {
        ArrayList<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(String.format(Locale.ROOT, fmt, i));
        return out;
    }

    /** Read /queries.json from classpath (src/main/resources/queries.json) */
    private List<Template> readTemplatesFromClasspath() {
        try (InputStream in = getResourceStream("queries.json")) {
            if (in == null) {
                throw new RuntimeException("queries.json not found on classpath (src/main/resources/queries.json)");
            }
            ObjectMapper om = new ObjectMapper();
            return om.readValue(in, new TypeReference<List<Template>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to load queries.json from classpath", e);
        }
    }

    private InputStream getResourceStream(String name) {
        // Try both relative and absolute classpath lookups
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        if (in == null) in = QueryGenerator.class.getClassLoader().getResourceAsStream(name);
        if (in == null) in = QueryGenerator.class.getResourceAsStream("/" + name);
        return in;
    }

    /* DTO mapping your JSON structure */
    public static class Template {
        public String name;
        public String category;
        public List<String> params;
        public String cypher;
        public String gremlin;

        @Override public String toString() {
            return (name == null ? "<unnamed>" : name) + " [" + category + "]";
        }
    }
}

