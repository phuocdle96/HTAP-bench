package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.benchmark.generator.QueryLanguage.CYPHER;
import static com.benchmark.generator.QueryLanguage.GREMLIN;

public class QueryGenerator {

    /** Pick which query dialects/templates to emit. */
    public enum Engine { NEO4J, MEMGRAPH, JANUSGRAPH }

    private final DatabaseClient db;
    private final Engine engine;

    private List<String> patientIds = List.of();
    private List<String> unitIds    = List.of();
    private List<String> diagCodes  = List.of();

    public QueryGenerator(DatabaseClient db, Engine engine) {
        this.db = db;
        this.engine = engine;
        loadSamples();
    }

    /** Build sufficiently large pools so workers/submitters always have params. */
    public Map<String, List<QueryTemplate.PreparedQuery>> prepareAllQueries() {
        Map<String, List<QueryTemplate.PreparedQuery>> m = new HashMap<>();
        m.put("OLTP",  buildOltpPool(10_000));
        m.put("GRAPH", buildGraphPool( 2_000));
        m.put("OLAP",  buildOlapPool(    500));
        return m;
    }

    /* -------------------- internals -------------------- */

    private void loadSamples() {
        try {
            patientIds = db.fetchSampleIds("Patient",        "patientId");
            unitIds    = db.fetchSampleIds("HealthcareUnit", "unitId");
            diagCodes  = db.fetchSampleIds("Diagnosis",      "code");
        } catch (Exception ignore) {
            // fall through to synthetic
        }
        if (patientIds.isEmpty()) patientIds = synthetic("P%07d", 10000);
        if (unitIds.isEmpty())    unitIds    = synthetic("U%05d", 1000);
        if (diagCodes.isEmpty())  diagCodes  = synthetic("D%04d", 500);
    }

    private static List<String> synthetic(String fmt, int n) {
        ArrayList<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(String.format(Locale.ROOT, fmt, i));
        return out;
    }

    private List<QueryTemplate.PreparedQuery> buildOltpPool(int n) {
        ArrayList<QueryTemplate.PreparedQuery> out = new ArrayList<>(n);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < n; i++) {
            String pid = patientIds.get(rnd.nextInt(patientIds.size()));

            if (engine == Engine.JANUSGRAPH) {
                // Gremlin version of "Patient Lookup"
                String g = """
                    g.V().has('Patient','patientId', pid)
                    .limit(1)
                    .valueMap('age','gender','nationality')
                    """;
                out.add(new QueryTemplate.PreparedQuery(GREMLIN, g, Map.of("pid", pid)));
            } else {
                // Cypher version (Neo4j / Memgraph)
                String c = "MATCH (p:Patient {patientId:$id}) RETURN p.age AS age, p.gender AS gender, p.nationality AS nationality";
                out.add(new QueryTemplate.PreparedQuery(CYPHER, c, Map.of("id", pid)));
            }
        }
        return out;
    }

    private List<QueryTemplate.PreparedQuery> buildGraphPool(int n) {
        ArrayList<QueryTemplate.PreparedQuery> out = new ArrayList<>(n);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < n; i++) {
            String pid = patientIds.get(rnd.nextInt(patientIds.size()));

            if (engine == Engine.JANUSGRAPH) {
                // Neighborhood around patient via events; adjust edge labels if yours differ.
                String g = """
                    g.V().has('Patient','patientId', pid)
                    .out('HAD_EVENT').hasLabel('Admission','Discharge')
                    .as('e')
                    .out('PERFORMED_AT','HAS_DIAGNOSIS')
                    .limit(20)
                    .valueMap(true)
                    """;
                out.add(new QueryTemplate.PreparedQuery(GREMLIN, g, Map.of("pid", pid)));
            } else {
                String c = """
                    MATCH (p:Patient {patientId:$id})-[:HAD_EVENT]->(e)
                    WHERE e:Admission OR e:Discharge
                    WITH e
                    MATCH (e)-[:PERFORMED_AT|:HAS_DIAGNOSIS]->(x)
                    RETURN x LIMIT 20
                    """;
                out.add(new QueryTemplate.PreparedQuery(CYPHER, c, Map.of("id", pid)));
            }
        }
        return out;
    }

    private List<QueryTemplate.PreparedQuery> buildOlapPool(int n) {
        ArrayList<QueryTemplate.PreparedQuery> out = new ArrayList<>(n);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < n; i++) {
            String unit = unitIds.get(rnd.nextInt(unitIds.size()));

            if (engine == Engine.JANUSGRAPH) {
                // Count admissions for a unit
                String g = """
                    g.V().has('HealthcareUnit','unitId', u)
                     .in('PERFORMED_AT')
                     .hasLabel('Admission')
                     .count()
                    """;
                out.add(new QueryTemplate.PreparedQuery(GREMLIN, g, Map.of("u", unit)));
            } else {
                String c = """
                    MATCH (:HealthcareUnit {unitId:$u})<-[:PERFORMED_AT]-(adm:Admission)
                    RETURN count(adm) AS c
                    """;
                out.add(new QueryTemplate.PreparedQuery(CYPHER, c, Map.of("u", unit)));
            }
        }
        return out;
    }
}
