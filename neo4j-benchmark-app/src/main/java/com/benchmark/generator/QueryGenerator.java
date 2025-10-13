package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.benchmark.generator.QueryLanguage.CYPHER;
import static com.benchmark.generator.QueryLanguage.GREMLIN;

public class QueryGenerator {

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

    public Map<String, List<QueryTemplate.PreparedQuery>> prepareAllQueries() {
        Map<String, List<QueryTemplate.PreparedQuery>> m = new HashMap<>();
        m.put("OLTP",  buildOltpPool(10_000));   // tune pool sizes as you like
        m.put("GRAPH", buildGraphPool( 2_000));
        m.put("OLAP",  buildOlapPool(    500));
        return m;
    }

    /* ---------------- Internal ---------------- */

    private void loadSamples() {
        // Try to gather enough IDs for randomization; fall back to synthetic values
        try {
            patientIds = db.fetchSampleIds("Patient", "patientId");
            unitIds    = db.fetchSampleIds("HealthcareUnit", "unitId");
            diagCodes  = db.fetchSampleIds("Diagnosis", "code");
        } catch (Exception ignore) { /* keep defaults if unavailable */ }

        if (patientIds.isEmpty()) patientIds = synthetic("P%05d", 1_000);
        if (unitIds.isEmpty())    unitIds    = synthetic("U%03d",  100);
        if (diagCodes.isEmpty())  diagCodes  = synthetic("D%03d",  500);
    }

    private static List<String> synthetic(String fmt, int n) {
        List<String> out = new ArrayList<>(n);
        for (int i=0;i<n;i++) out.add(String.format(Locale.ROOT, fmt, i));
        return out;
    }

    private List<QueryTemplate.PreparedQuery> buildOltpPool(int n) {
        List<QueryTemplate.PreparedQuery> out = new ArrayList<>(n);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i=0;i<n;i++) {
            String id = patientIds.get(rnd.nextInt(patientIds.size()));
            if (engine == Engine.JANUSGRAPH) {
                String g = "g.V().has('Patient','patientId', id).limit(1).valueMap(true)";
                out.add(new QueryTemplate.PreparedQuery(GREMLIN, g, Map.of("id", id)));
            } else {
                String c = "MATCH (p:Patient {patientId:$id}) RETURN p LIMIT 1";
                out.add(new QueryTemplate.PreparedQuery(CYPHER, c, Map.of("id", id)));
            }
        }
        return out;
    }

    private List<QueryTemplate.PreparedQuery> buildGraphPool(int n) {
        List<QueryTemplate.PreparedQuery> out = new ArrayList<>(n);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i=0;i<n;i++) {
            String id = patientIds.get(rnd.nextInt(patientIds.size()));
            if (engine == Engine.JANUSGRAPH) {
                String g = "g.V().has('Patient','patientId', id).both('VISITED','DIAGNOSED').limit(20).valueMap(true)";
                out.add(new QueryTemplate.PreparedQuery(GREMLIN, g, Map.of("id", id)));
            } else {
                String c = "MATCH (p:Patient {patientId:$id})-[:VISITED|:DIAGNOSED]->(x) RETURN x LIMIT 20";
                out.add(new QueryTemplate.PreparedQuery(CYPHER, c, Map.of("id", id)));
            }
        }
        return out;
    }

    private List<QueryTemplate.PreparedQuery> buildOlapPool(int n) {
        List<QueryTemplate.PreparedQuery> out = new ArrayList<>(n);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i=0;i<n;i++) {
            String u = unitIds.get(rnd.nextInt(unitIds.size()));
            if (engine == Engine.JANUSGRAPH) {
                String g = "g.V().has('HealthcareUnit','unitId', u).in('ADMITTED_TO').count()";
                out.add(new QueryTemplate.PreparedQuery(GREMLIN, g, Map.of("u", u)));
            } else {
                String c = "MATCH (:HealthcareUnit {unitId:$u})<-[:ADMITTED_TO]-(:Admission) RETURN count(*) AS c";
                out.add(new QueryTemplate.PreparedQuery(CYPHER, c, Map.of("u", u)));
            }
        }
        return out;
    }
}
