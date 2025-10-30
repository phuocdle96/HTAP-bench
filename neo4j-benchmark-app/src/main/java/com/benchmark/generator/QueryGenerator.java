// src/main/java/com/benchmark/generator/QueryGenerator.java
package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.benchmark.generator.QueryLanguage.CYPHER;
import static com.benchmark.generator.QueryLanguage.GREMLIN;

/**
 * QueryGenerator that loads templates from classpath resource: src/main/resources/queries.json
 * and binds parameters using sampled IDs from the database (with synthetic fallbacks).
 *
 * Engine selection:
 *  - NEO4J / MEMGRAPH -> uses Template.cypher and binds java.time.LocalDate for date params
 *  - JANUSGRAPH       -> uses Template.gremlin and binds YYYYMMDD as long for date params
 *
 * Dataset date range assumption: 2000–2012 inclusive.
 * We generate a random month window inside this range and provide:
 *   - startDate: first day of the month (inclusive)
 *   - endDate:   first day of next month (exclusive)
 *
 * Param binding summary:
 *  - patientId/unitId/diagnosisCode: sampled from DB (fallback to synthetic)
 *  - admissionId: random UUID
 *  - admissionDate:
 *        Neo4j/Memgraph -> LocalDate
 *        JanusGraph     -> long YYYYMMDD (e.g., 20090101L)
 *  - startDate/endDate:
 *        Neo4j/Memgraph -> LocalDate
 *        JanusGraph     -> long YYYYMMDD
 *  - windowDays (for readmission analysis): random between 7 and 30 (inclusive)
 */
public class QueryGenerator {

    /** Pick which query dialects/templates to emit. */
    public enum Engine { NEO4J, MEMGRAPH, JANUSGRAPH }

    private static final int MIN_YEAR = 2000;
    private static final int MAX_YEAR = 2012;

    private final DatabaseClient db;
    private final Engine engine;

    /* Sample pools for param binding */
    private List<String> patientIds = List.of();
    private List<String> unitIds    = List.of();
    private List<String> diagCodes  = List.of();

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

            // Normalize Gremlin once per template for JanusGraph/TinkerPop 3.7 wording.
            if (engine == Engine.JANUSGRAPH) {
                text = normalizeGremlin(text);
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

    /** Normalize Gremlin text (e.g., incr/decr → asc/desc). */
    private static String normalizeGremlin(String s) {
        if (s == null) return null;
        String q = s;
        q = q.replaceAll("(?<!\\w)incr(?!\\w)", "asc");
        q = q.replaceAll("(?<!\\w)decr(?!\\w)", "desc");
        q = q.replaceAll("\\.by\\s*\\(\\s*values\\s*,\\s*incr\\s*\\)", ".by(values, asc)");
        q = q.replaceAll("\\.by\\s*\\(\\s*values\\s*,\\s*desc\\s*\\)", ".by(values, desc)");
        q = q.replaceAll("\\.by\\s*\\(\\s*values\\s*,\\s*decr\\s*\\)", ".by(values, desc)");
        q = q.replaceAll("\\.by\\s*\\(\\s*keys\\s*,\\s*incr\\s*\\)",   ".by(keys, asc)");
        q = q.replaceAll("\\.by\\s*\\(\\s*keys\\s*,\\s*decr\\s*\\)",   ".by(keys, desc)");
        return q;
    }

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

        // Precompute a month window once per query execution (if any date params are present).
        final YearMonth ym = randomYearMonth(rnd);
        final LocalDate startLD = LocalDate.of(ym.getYear(), ym.getMonth(), 1);
        final LocalDate endLD   = startLD.plusMonths(1);

        // Engine-specific representations for range params
        final Object startForEngine = switch (engine) {
            case JANUSGRAPH -> toYYYYMMDDLong(startLD);
            case NEO4J, MEMGRAPH -> startLD;
        };
        final Object endForEngine = switch (engine) {
            case JANUSGRAPH -> toYYYYMMDDLong(endLD);
            case NEO4J, MEMGRAPH -> endLD;
        };

        for (String n : names) {
            switch (n) {
                case "patientId"     -> p.put("patientId", pick(patientIds, rnd));
                case "unitId"        -> p.put("unitId", pick(unitIds, rnd));
                case "diagnosisCode" -> p.put("diagnosisCode", pick(diagCodes, rnd));

                case "admissionId"   -> p.put("admissionId", UUID.randomUUID().toString());

                case "admissionDate" -> {
                    // If an OLTP insert needs a date property:
                    // - Neo4j/Memgraph -> LocalDate
                    // - JanusGraph     -> long YYYYMMDD
                    LocalDate day = randomDayInMonth(ym, rnd);
                    if (engine == Engine.JANUSGRAPH) {
                        p.put("admissionDate", toYYYYMMDDLong(day));
                    } else {
                        p.put("admissionDate", day);
                    }
                }

                // Range params used by OLAP templates (month window)
                case "startDate" -> p.put("startDate", startForEngine);
                case "endDate"   -> p.put("endDate",   endForEngine);

                // Readmission window (GRAPH workload)
                case "windowDays" -> p.put("windowDays", rnd.nextInt(7, 31));

                // Backward compatibility if any legacy template still uses 'year'
                case "year"      -> p.put("year", String.valueOf(rnd.nextInt(MIN_YEAR, MAX_YEAR + 1)));

                default -> {
                    // Fallback: generate a neutral placeholder that won't coerce to wrong numeric type.
                    // (All current templates bind known names, so this path shouldn't be used.)
                    p.put(n, "X" + rnd.nextInt(1_000_000));
                }
            }
        }
        return p;
    }

    private static <T> T pick(List<T> list, ThreadLocalRandom rnd) {
        return list.get(rnd.nextInt(list.size()));
    }

    /** Load ID samples from the DB; fallback to synthetic if empty. */
    private void loadSamples() {
        try {
            patientIds = db.fetchSampleIds("Patient",        "patientId");
            unitIds    = db.fetchSampleIds("HealthcareUnit", "unitId");
            diagCodes  = db.fetchSampleIds("Diagnosis",      "code");
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

    /* ===== helpers for date handling ===== */

    private static YearMonth randomYearMonth(ThreadLocalRandom rnd) {
        int year  = rnd.nextInt(MIN_YEAR, MAX_YEAR + 1);
        int month = rnd.nextInt(1, 13);
        return YearMonth.of(year, month);
    }

    private static LocalDate randomDayInMonth(YearMonth ym, ThreadLocalRandom rnd) {
        int day = rnd.nextInt(1, Math.min(28, ym.lengthOfMonth()) + 1);
        return LocalDate.of(ym.getYear(), ym.getMonthValue(), day);
    }

    /** Convert LocalDate to numeric YYYYMMDD (long), e.g., 20090101L. */
    private static long toYYYYMMDDLong(LocalDate d) {
        return d.getYear() * 10000L + d.getMonthValue() * 100L + d.getDayOfMonth();
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
