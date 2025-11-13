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
 * Loads query templates from classpath: src/main/resources/queries.json
 * and binds parameters using sampled IDs (with synthetic fallbacks).
 *
 * Engine selection:
 *  - NEO4J      -> Template.cypher
 *  - MEMGRAPH   -> Template.memgraph (if provided) else Template.cypher
 *  - JANUSGRAPH -> Template.gremlin
 *
 * Dates:
 *  - Neo4j & Memgraph use String "yyyyMMdd"
 *  - JanusGraph uses long yyyymmdd
 *
 * Heartbeat (HB.*) templates are skipped here (HeartbeatService handles them).
 * NOTE: Gremlin reserves parameter name "id" at the protocol layer. Do not use it in queries.json.
 */
public class QueryGenerator {

    /** Supported engines */
    public enum Engine { NEO4J, MEMGRAPH, JANUSGRAPH }

    private static final int MIN_YEAR = 2000;
    private static final int MAX_YEAR = 2012;

    private final DatabaseClient db;
    private final Engine engine;

    /* Sample pools for param binding */
    private List<String> patientIds         = List.of();
    private List<String> unitIds            = List.of();
    private List<String> diagCodes          = List.of();
    private List<String> admissionEventIds  = List.of(); // spread writes across many admissions

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
            if (t.name != null && t.name.startsWith("HB.")) continue; // Heartbeat handled elsewhere

            String cat = (t.category == null) ? "OLTP" : t.category.toUpperCase(Locale.ROOT);
            String text = chooseTextForEngine(t);
            if (text == null || text.isBlank()) continue;

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

    /* ============================ helpers ============================ */

    private String chooseTextForEngine(Template t) {
        return switch (engine) {
            case NEO4J      -> t.cypher;
            case MEMGRAPH   -> (t.memgraph != null && !t.memgraph.isBlank()) ? t.memgraph : t.cypher;
            case JANUSGRAPH -> t.gremlin;
        };
    }

    private Map<String,Object> bindParams(List<String> names, ThreadLocalRandom rnd) {
        if (names == null || names.isEmpty()) return Map.of();

        Map<String,Object> p = new HashMap<>();

        // One month window per query instance
        final YearMonth ym      = randomYearMonth(rnd);
        final LocalDate startLD = LocalDate.of(ym.getYear(), ym.getMonth(), 1);
        final LocalDate endLD   = startLD.plusMonths(1);

        final Object startForEngine = (engine == Engine.JANUSGRAPH) ? toYYYYMMDDLong(startLD) : yyyymmdd(startLD);
        final Object endForEngine   = (engine == Engine.JANUSGRAPH) ? toYYYYMMDDLong(endLD)   : yyyymmdd(endLD);

        for (String n : names) {
            switch (n) {
                // IDs from DB samples or synthetic
                case "patientId"          -> p.put("patientId", pick(patientIds, rnd));
                case "unitId"             -> p.put("unitId", pick(unitIds, rnd));
                case "diagnosisCode"      -> p.put("diagnosisCode", pick(diagCodes, rnd));
                case "admissionEventId"   -> p.put("admissionEventId", pick(admissionEventIds, rnd));
                case "eventId"            -> p.put("eventId", UUID.randomUUID().toString()); // fallback for CREATE
                case "newEventId"         -> p.put("newEventId", UUID.randomUUID().toString());
                // IMPORTANT: Do NOT use "id" as a binding; Gremlin treats it as reserved.

                // Dates
                case "admissionDate" -> {
                    LocalDate d = randomDayInMonth(ym, rnd);
                    p.put("admissionDate", engine == Engine.JANUSGRAPH ? toYYYYMMDDLong(d) : yyyymmdd(d));
                }
                case "chemoDate" -> {
                    LocalDate d = randomDayInMonth(ym, rnd);
                    p.put("chemoDate", engine == Engine.JANUSGRAPH ? toYYYYMMDDLong(d) : yyyymmdd(d));
                }
                case "dischargeDate" -> {
                    LocalDate d = randomDayInMonth(ym, rnd);
                    p.put("dischargeDate", engine == Engine.JANUSGRAPH ? toYYYYMMDDLong(d) : yyyymmdd(d));
                }
                case "startDate" -> p.put("startDate", startForEngine);
                case "endDate"   -> p.put("endDate",   endForEngine);

                // bounded integers
                case "limit"      -> p.put("limit", rnd.nextInt(10, 51));
                case "k"          -> p.put("k", rnd.nextInt(5, 51));
                case "maxHops"    -> p.put("maxHops", rnd.nextInt(1, 4));
                case "windowDays" -> p.put("windowDays", rnd.nextInt(7, 31));
                case "age"        -> p.put("age", rnd.nextInt(0, 101));

                // small-domain strings
                case "gender"         -> p.put("gender", rnd.nextBoolean() ? "M" : "F");
                case "nationality"    -> p.put("nationality", String.format(Locale.ROOT, "%03d", rnd.nextInt(1, 201)));
                case "education"      -> p.put("education", List.of("NONE","HS","COLLEGE","GRAD").get(rnd.nextInt(4)));
                case "specialty"      -> p.put("specialty", String.format(Locale.ROOT, "%02d", rnd.nextInt(1, 18)));
                case "schema"         -> p.put("schema", rnd.nextBoolean() ? "TMX" : "GENCI");
                case "diagnosisType"  -> p.put("diagnosisType", "SECONDARY");
                case "deathIndicator" -> p.put("deathIndicator", rnd.nextBoolean() ? "1" : "0");
                case "year"           -> p.put("year", String.valueOf(rnd.nextInt(MIN_YEAR, MAX_YEAR + 1)));

                default -> p.put(n, "X" + rnd.nextInt(1_000_000)); // safe fallback for unused params
            }
        }
        return p;
    }

    private static <T> T pick(List<T> list, ThreadLocalRandom rnd) {
        return list.get(rnd.nextInt(list.size()));
    }

    private void loadSamples() {
        try {
            patientIds        = db.fetchSampleIds("Patient", "patientId");
            unitIds           = db.fetchSampleIds("HealthcareUnit", "unitId");
            diagCodes         = db.fetchSampleIds("Diagnosis", "code");
            // For JanusGraph, "Event:Admission" is supported in JanusGraphClient.fetchSampleIds
            admissionEventIds = db.fetchSampleIds("Event:Admission", "eventId");
        } catch (Exception ignore) {}

        if (patientIds == null || patientIds.isEmpty())        patientIds = synthetic("P%08d", 50_000);
        if (unitIds == null || unitIds.isEmpty())              unitIds    = synthetic("U%05d", 1_000);
        if (diagCodes == null || diagCodes.isEmpty())          diagCodes  = synthetic("D%05d", 5_000);
        if (admissionEventIds == null || admissionEventIds.isEmpty())
            admissionEventIds = synthetic("A%012d", 200_000);
    }

    private static List<String> synthetic(String fmt, int n) {
        ArrayList<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(String.format(Locale.ROOT, fmt, i));
        return out;
    }

    private List<Template> readTemplatesFromClasspath() {
        try (InputStream in = getResourceStream("queries.json")) {
            if (in == null) throw new RuntimeException("queries.json not found on classpath");
            ObjectMapper om = new ObjectMapper();
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

    /* DTO for queries.json */
    public static class Template {
        public String name;
        public String category;
        public List<String> params;
        public String cypher;
        public String memgraph; // Memgraph-specific Cypher
        public String gremlin;

        @Override public String toString() {
            return (name == null ? "<unnamed>" : name) + " [" + category + "]";
        }
    }
}

