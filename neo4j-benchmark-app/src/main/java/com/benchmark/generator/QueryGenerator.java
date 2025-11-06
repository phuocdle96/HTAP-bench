// src/main/java/com/benchmark/generator/QueryGenerator.java
package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.benchmark.generator.QueryLanguage;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.benchmark.generator.QueryLanguage.CYPHER;
import static com.benchmark.generator.QueryLanguage.GREMLIN;

/**
 * QueryGenerator loads templates from classpath: src/main/resources/queries.json
 * and binds parameters using sampled IDs from the database (with synthetic fallbacks).
 *
 * Engine selection:
 *  - NEO4J      -> uses Template.cypher; dates bound as java.time.LocalDate
 *  - MEMGRAPH   -> uses Template.cypher but REWRITTEN to single-label (:Event {subtype:'X'})
 *                  and dates bound as strings "yyyyMMdd" (lexicographic-safe)
 *  - JANUSGRAPH -> uses Template.gremlin (normalized), dates bound as long YYYYMMDDL
 *
 * Dataset date range assumption: 2000–2012 inclusive.
 * We pick a random month window and provide:
 *   - startDate/endDate: engine-specific representation (see above)
 *   - For Memgraph we ALSO bind startDateStr/endDateStr (yyyyMMdd).
 *
 * IMPORTANT for Memgraph:
 *   Your data model has a single :Event label with property `subtype` ("Admission", "Discharge", ...),
 *   and `date` is stored as a string "YYYYMMDD".
 *   This class rewrites Neo4j-style patterns like:
 *     MATCH (adm:Admission)-[:PERFORMED_AT]->(hcu:HealthcareUnit)
 *   into:
 *     MATCH (adm:Event {subtype:'Admission'})-[:PERFORMED_AT]->(hcu:HealthcareUnit)
 *   and converts date range parameters to $startDateStr/$endDateStr (string).
 *   It also rewrites INSERT patterns that create (:Event:Admission {date:$admissionDate})
 *   into (:Event {subtype:'Admission', date:$admissionDateStr}).
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
            // 1) Skip heartbeat templates so they don’t affect throughput
            if (t.name != null && t.name.startsWith("HB.")) {
                continue;
            }

            String cat = (t.category == null) ? "OLTP" : t.category.toUpperCase(Locale.ROOT);
            String text = chooseTextForEngine(t);
            if (text == null || text.isBlank()) {
                continue;
            }

            // Normalize/Rewrite once per template
            if (engine == Engine.JANUSGRAPH) {
                text = normalizeGremlin(text);
            } else if (engine == Engine.MEMGRAPH) {
                text = transformCypherForMemgraph(text);
            }

            int copies = switch (cat) {
                case "OLTP"  -> 10_000;
                case "GRAPH" ->  2_000;  // <-- fixed literal (underscore)
                case "OLAP"  ->    500;
                default      ->    500;
            };

            for (int i = 0; i < copies; i++) {
                Map<String,Object> params = bindParams(t.params, rnd);
                QueryLanguage lang = (engine == Engine.JANUSGRAPH) ? GREMLIN : CYPHER;

                // 2) Carry name/category (helps logs and any per-name logic)
                out.get(cat).add(
                    new QueryTemplate.PreparedQuery(lang, text, params, t.name, cat)
                );
            }
        }

        return out;
    }

    /* ============================ internals ============================ */

    /** Normalize Gremlin text (e.g., incr/decr → asc/desc) for TinkerPop 3.7+ wordings. */
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

    /**
     * MEMGRAPH REWRITE:
     *  - (:Admission) → (:Event {subtype:'Admission'})
     *  - (:Discharge) → (:Event {subtype:'Discharge'})
     *  - CREATE (:Event:Admission { ... date:$admissionDate ... })
     *        → CREATE (:Event {subtype:'Admission', ... date:$admissionDateStr ...})
     *  - WHERE adm.date >= $startDate AND adm.date < $endDate
     *        → WHERE adm.date >= $startDateStr AND adm.date < $endDateStr
     *  - MATCH (e:Event)-[:HAS_DIAGNOSIS]-> ...  stays the same
     *    (no change needed unless label-specific)
     */
    private static String transformCypherForMemgraph(String cypher) {
        if (cypher == null || cypher.isBlank()) return cypher;
        String q = cypher;

        // Common label → subtype rewrites
        q = q.replaceAll("\\(\\s*(\\w+)\\s*:\\s*Admission\\s*\\)", "($1:Event {subtype:'Admission'})");
        q = q.replaceAll("\\(\\s*(\\w+)\\s*:\\s*Discharge\\s*\\)", "($1:Event {subtype:'Discharge'})");

        // CREATE (:Event:Admission { ... }) → (:Event {subtype:'Admission', ...})
        q = q.replaceAll("\\(:\\s*Event\\s*:\\s*Admission\\s*\\{", "(:Event {subtype:'Admission', ");
        q = q.replaceAll("\\(:\\s*Event\\s*:\\s*Discharge\\s*\\{", "(:Event {subtype:'Discharge', ");

        // date param names: for range queries switch to string params
        // WHERE adm.date >= $startDate AND adm.date < $endDate
        q = q.replaceAll("(?i)\\badm\\.date\\s*>=\\s*\\$startDate\\b", "adm.date >= $startDateStr");
        q = q.replaceAll("(?i)\\badm\\.date\\s*<\\s*\\$endDate\\b",    "adm.date < $endDateStr");

        // INSERT path: use admissionDateStr when present
        q = q.replaceAll("(?i)date\\s*:\\s*\\$admissionDate\\b", "date: $admissionDateStr");

        // If someone used label in RETURN/ORDER BY, nothing to do; properties unaffected.

        return q;
    }

    /** Bind well-known parameters (fallback to synthetic if samples are empty). */
    private Map<String,Object> bindParams(List<String> names, ThreadLocalRandom rnd) {
        if (names == null || names.isEmpty()) return Map.of();

        Map<String,Object> p = new HashMap<>();

        // Precompute a month window once per query execution
        final YearMonth ym   = randomYearMonth(rnd);
        final LocalDate startLD = LocalDate.of(ym.getYear(), ym.getMonth(), 1);
        final LocalDate endLD   = startLD.plusMonths(1);

        // Engine-specific representations for range params
        final Object startForEngine = switch (engine) {
            case JANUSGRAPH -> toYYYYMMDDLong(startLD);
            case NEO4J      -> startLD;
            case MEMGRAPH   -> null; // use string form below
        };
        final Object endForEngine = switch (engine) {
            case JANUSGRAPH -> toYYYYMMDDLong(endLD);
            case NEO4J      -> endLD;
            case MEMGRAPH   -> null; // use string form below
        };

        // Precompute string date forms for Memgraph ("yyyyMMdd")
        final String startStr = yyyymmdd(startLD);
        final String endStr   = yyyymmdd(endLD);

        for (String n : names) {
            switch (n) {
                case "patientId"     -> p.put("patientId", pick(patientIds, rnd));
                case "unitId"        -> p.put("unitId", pick(unitIds, rnd));
                case "diagnosisCode" -> p.put("diagnosisCode", pick(diagCodes, rnd));

                case "admissionId"   -> p.put("admissionId", UUID.randomUUID().toString());

                case "admissionDate" -> {
                    LocalDate day = randomDayInMonth(ym, rnd);
                    if (engine == Engine.JANUSGRAPH) {
                        p.put("admissionDate", toYYYYMMDDLong(day));
                    } else if (engine == Engine.MEMGRAPH) {
                        // We ALSO add "admissionDateStr" below outside the switch.
                        p.put("admissionDate", day); // harmless if not used by rewritten query
                    } else {
                        p.put("admissionDate", day);
                    }
                    // Always supply a string form for Memgraph convenience
                    if (engine == Engine.MEMGRAPH) {
                        p.put("admissionDateStr", yyyymmdd(day));
                    }
                }

                // Range params used by OLAP templates (month window)
                case "startDate" -> {
                    if (engine == Engine.JANUSGRAPH) p.put("startDate", startForEngine);
                    else if (engine == Engine.NEO4J) p.put("startDate", startForEngine);
                    // Memgraph uses string params (rewritten query)
                    if (engine == Engine.MEMGRAPH)  p.put("startDateStr", startStr);
                }
                case "endDate" -> {
                    if (engine == Engine.JANUSGRAPH) p.put("endDate", endForEngine);
                    else if (engine == Engine.NEO4J) p.put("endDate", endForEngine);
                    if (engine == Engine.MEMGRAPH)   p.put("endDateStr", endStr);
                }

                // Readmission window (GRAPH workload)
                case "windowDays" -> p.put("windowDays", rnd.nextInt(7, 31));

                // Back-compat
                case "year"      -> p.put("year", String.valueOf(rnd.nextInt(MIN_YEAR, MAX_YEAR + 1)));

                default -> p.put(n, "X" + rnd.nextInt(1_000_000));
            }
        }

        // Safety: if template included startDate/endDate but engine==MEMGRAPH, ensure string params exist.
        if (engine == Engine.MEMGRAPH && names.stream().anyMatch(s -> s.equals("startDate") || s.equals("endDate"))) {
            p.putIfAbsent("startDateStr", startStr);
            p.putIfAbsent("endDateStr",   endStr);
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

    /** Format LocalDate as "yyyyMMdd". */
    private static String yyyymmdd(LocalDate d) {
        int y = d.getYear();
        int m = d.getMonthValue();
        int day = d.getDayOfMonth();
        // zero-pad month/day
        return String.format(Locale.ROOT, "%04d%02d%02d", y, m, day);
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
