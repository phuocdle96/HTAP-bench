package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.benchmark.generator.QueryLanguage.CYPHER;
import static com.benchmark.generator.QueryLanguage.GREMLIN;

/**
 * Loads query templates from classpath: src/main/resources/queries.json
 * and binds parameters using sampled IDs (with synthetic fallbacks).
 *
 * Engine selection:
 *  - NEO4J      -> Template.cypher; dates as "YYYYMMDD" strings (per user dataset)
 *  - MEMGRAPH   -> Template.cypher transformed as needed; dates as "YYYYMMDD" strings
 *  - JANUSGRAPH -> Template.gremlin (normalized), dates as long YYYYMMDDL
 *
 * HB templates are purposely SKIPPED here (HeartbeatService runs HB separately).
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

    // Lightweight value pools (match your dataset style)
    private static final List<String> GENDERS       = List.of("M", "F");
    private static final List<String> NATIONALITIES = List.of("010","020","030","040","050","060","070","080","090");
    private static final List<String> SPECIALTIES   = List.of("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16");
    private static final List<String> CHEMO_SCHEMAS = List.of("TMX","GENCI","ABVD","FOLFOX","CHOP","R-CHOP");

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
            // 🚫 Skip any heartbeat templates (names starting with "HB.")
            if (t.name != null && t.name.startsWith("HB.")) continue;

            String cat = (t.category == null) ? "OLTP" : t.category.toUpperCase(Locale.ROOT);
            String text = chooseTextForEngine(t);
            if (text == null || text.isBlank()) continue;

            if (engine == Engine.JANUSGRAPH) {
                text = normalizeGremlin(text);
            } else if (engine == Engine.MEMGRAPH) {
                text = transformCypherForMemgraph(text);
            } else {
                // Neo4j: no rewrite; dates are bound below.
            }

            int copies = switch (cat) {
                case "OLTP"  -> 10000;
                case "GRAPH" -> 2000;
                case "OLAP"  ->  500;
                default      ->  500;
            };

            for (int i = 0; i < copies; i++) {
                Map<String,Object> params = bindParams(t.params, rnd);
                QueryLanguage lang = (engine == Engine.JANUSGRAPH) ? GREMLIN : CYPHER;
                out.get(cat).add(new QueryTemplate.PreparedQuery(lang, text, params, t.name, cat));
            }
        }
        return out;
    }

    /* ============================ internals ============================ */

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

    /** MEMGRAPH REWRITE: (:Admission) → (:Event {subtype:'Admission'}), date params to strings, etc. */
    private static String transformCypherForMemgraph(String cypher) {
        if (cypher == null || cypher.isBlank()) return cypher;
        String q = cypher;

        // Common label → subtype rewrites
        q = q.replaceAll("\\(\\s*(\\w+)\\s*:\\s*Admission\\s*\\)", "($1:Event {subtype:'Admission'})");
        q = q.replaceAll("\\(\\s*(\\w+)\\s*:\\s*Discharge\\s*\\)", "($1:Event {subtype:'Discharge'})");

        // CREATE (:Event:Admission {...}) → (:Event {subtype:'Admission', ...})
        q = q.replaceAll("\\(:\\s*Event\\s*:\\s*Admission\\s*\\{", "(:Event {subtype:'Admission', ");
        q = q.replaceAll("\\(:\\s*Event\\s*:\\s*Discharge\\s*\\{", "(:Event {subtype:'Discharge', ");

        // WHERE adm.date >= $startDate AND adm.date < $endDate  → string params
        q = q.replaceAll("(?i)\\badm\\.date\\s*>=\\s*\\$startDate\\b", "adm.date >= $startDateStr");
        q = q.replaceAll("(?i)\\badm\\.date\\s*<\\s*\\$endDate\\b",    "adm.date < $endDateStr");

        // INSERT path: use admissionDateStr when present
        q = q.replaceAll("(?i)date\\s*:\\s*\\$admissionDate\\b", "date: $admissionDateStr");

        return q;
    }

    private Map<String,Object> bindParams(List<String> names, ThreadLocalRandom rnd) {
        if (names == null || names.isEmpty()) return Map.of();

        Map<String,Object> p = new HashMap<>();

        // Precompute a month window once per query execution
        final YearMonth ym   = randomYearMonth(rnd);
        final LocalDate startLD = LocalDate.of(ym.getYear(), ym.getMonth(), 1);
        final LocalDate endLD   = startLD.plusMonths(1);

        final Object startForEngine = switch (engine) {
            case JANUSGRAPH -> toYYYYMMDDLong(startLD);
            case NEO4J      -> null; // Neo4j will use string below
            case MEMGRAPH   -> null; // Memgraph uses string below
        };
        final Object endForEngine = switch (engine) {
            case JANUSGRAPH -> toYYYYMMDDLong(endLD);
            case NEO4J      -> null;
            case MEMGRAPH   -> null;
        };

        final String startStr = yyyymmdd(startLD);
        final String endStr   = yyyymmdd(endLD);

        for (String n : names) {
            switch (n) {
                // ---- Entity identifiers ----
                case "id" -> // alias used in 1.1/1.1b; must be patientId
                    p.put("id", pick(patientIds, rnd));
                case "patientId" ->
                    p.put("patientId", pick(patientIds, rnd));
                case "unitId" ->
                    p.put("unitId", pick(unitIds, rnd));
                case "diagnosisCode" ->
                    p.put("diagnosisCode", pick(diagCodes, rnd));
                case "eventId" ->
                    p.put("eventId", UUID.randomUUID().toString());
                case "admissionId" ->
                    p.put("admissionId", UUID.randomUUID().toString());

                // ---- Demographics / attributes ----
                case "age" ->
                    p.put("age", rnd.nextInt(1, 100)); // Integer
                case "gender" ->
                    p.put("gender", pick(GENDERS, rnd));
                case "nationality", "newNationality" ->
                    p.put(n, pick(NATIONALITIES, rnd));
                case "education" ->
                    p.put("education", String.valueOf(rnd.nextInt(1, 8)));
                case "specialty" ->
                    p.put("specialty", pick(SPECIALTIES, rnd));
                case "schema" ->
                    p.put("schema", pick(CHEMO_SCHEMAS, rnd));
                case "diagnosisType" ->
                    p.put("diagnosisType", rnd.nextBoolean() ? "PRIMARY" : "SECONDARY");

                // ---- Dates ----
                case "admissionDate", "chemoDate", "dischargeDate" -> {
                    LocalDate day = randomDayInMonth(ym, rnd);
                    if (engine == Engine.JANUSGRAPH) {
                        p.put(n, toYYYYMMDDLong(day));        // long like 20110614L
                    } else {
                        String d = yyyymmdd(day);              // "yyyymmdd" for Neo4j/Memgraph
                        p.put(n, d);
                        if (n.equals("admissionDate")) {
                            // Memgraph transformed query reads admissionDateStr
                            p.put("admissionDateStr", d);
                        }
                    }
                }
                case "startDate" -> {
                    if (engine == Engine.JANUSGRAPH) p.put("startDate", startForEngine);
                    else if (engine == Engine.NEO4J) p.put("startDate", startStr);
                    if (engine == Engine.MEMGRAPH)   p.put("startDateStr", startStr);
                }
                case "endDate" -> {
                    if (engine == Engine.JANUSGRAPH) p.put("endDate", endForEngine);
                    else if (engine == Engine.NEO4J) p.put("endDate", endStr);
                    if (engine == Engine.MEMGRAPH)   p.put("endDateStr", endStr);
                }
                case "year" ->
                    p.put("year", String.valueOf(rnd.nextInt(MIN_YEAR, MAX_YEAR + 1))); // used with STARTS WITH

                // ---- Graph bounds / numeric knobs ----
                case "limit" ->
                    p.put("limit", rnd.nextInt(5, 51)); // Integer, fixes OPEN/RAMP-UP type mismatch
                case "k" ->
                    p.put("k", rnd.nextInt(1, 51));     // Integer
                case "maxHops" ->
                    p.put("maxHops", rnd.nextInt(1, 5)); // 1..4
                case "windowDays" ->
                    p.put("windowDays", rnd.nextInt(7, 31)); // Integer

                // ---- Fallback (harmless string token) ----
                default ->
                    p.put(n, "X" + rnd.nextInt(1_000_000));
            }
        }

        // Safety for Memgraph range queries
        if (engine == Engine.MEMGRAPH && names.stream().anyMatch(s -> s.equals("startDate") || s.equals("endDate"))) {
            p.putIfAbsent("startDateStr", startStr);
            p.putIfAbsent("endDateStr",   endStr);
        }

        return p;
    }

    private static <T> T pick(List<T> list, ThreadLocalRandom rnd) {
        return list.get(rnd.nextInt(list.size()));
    }

    private void loadSamples() {
        try {
            patientIds = db.fetchSampleIds("Patient",        "patientId");
            unitIds    = db.fetchSampleIds("HealthcareUnit", "unitId");
            diagCodes  = db.fetchSampleIds("Diagnosis",      "code");
        } catch (Exception ignore) {}

        if (patientIds == null || patientIds.isEmpty()) patientIds = synthetic("P%08d",  50_000);
        if (unitIds    == null || unitIds.isEmpty())    unitIds    = synthetic("U%05d",   1_000);
        if (diagCodes  == null || diagCodes.isEmpty())  diagCodes  = synthetic("D%05d",   5_000);
    }

    private static List<String> synthetic(String fmt, int n) {
        ArrayList<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(String.format(Locale.ROOT, fmt, i));
        return out;
    }

    private List<Template> readTemplatesFromClasspath() {
        try (InputStream in = getResourceStream("queries.json")) {
            if (in == null) throw new RuntimeException("queries.json not found on classpath");

            // Tolerate comments/trailing commas so tests don't die on tiny edits
            JsonFactory jf = JsonFactory.builder()
                    .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
                    .enable(JsonReadFeature.ALLOW_YAML_COMMENTS)
                    .enable(JsonReadFeature.ALLOW_TRAILING_COMMA)
                    .build();
            ObjectMapper om = new ObjectMapper(jf);

            return om.readValue(in, new TypeReference<List<Template>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to load queries.json", e);
        }
    }

    private InputStream getResourceStream(String name) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        if (in == null) in = QueryGenerator.class.getClassLoader().getResourceAsStream(name);
        if (in == null) in = QueryGenerator.class.getResourceAsStream("/" + name);
        return in;
    }

    private static YearMonth randomYearMonth(ThreadLocalRandom rnd) {
        int year  = rnd.nextInt(MIN_YEAR, MAX_YEAR + 1);
        int month = rnd.nextInt(1, 13);
        return YearMonth.of(year, month);
    }

    private static LocalDate randomDayInMonth(YearMonth ym, ThreadLocalRandom rnd) {
        int day = rnd.nextInt(1, Math.min(28, ym.lengthOfMonth()) + 1);
        return LocalDate.of(ym.getYear(), ym.getMonthValue(), day);
    }

    private static long toYYYYMMDDLong(LocalDate d) {
        return d.getYear() * 10000L + d.getMonthValue() * 100L + d.getDayOfMonth();
    }

    private static String yyyymmdd(LocalDate d) {
        return String.format(Locale.ROOT, "%04d%02d%02d", d.getYear(), d.getMonthValue(), d.getDayOfMonth());
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

