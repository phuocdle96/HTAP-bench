package com.benchmark.freshness;

import com.benchmark.client.DatabaseClient;
import com.benchmark.generator.QueryGenerator;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Heartbeat writer + sampler for freshness measurement.
 *
 * - Writes tiny "heartbeat" Events at a low fixed rate (default 2s) using the SAME
 *   labels/properties/indexed predicates that OLAP queries use per engine.
 * - Samples the most recent visible heartbeat via the SAME predicates to compute:
 *      freshness = now_ms - ts_write_ms_of_latest_visible_heartbeat
 * - Logs to heartbeat_freshness.csv and prints a short percentile summary on stop().
 *
 * Notes on per-engine mapping:
 *   Neo4j:     (:Event:Heartbeat {date: LocalDate, subtype:'Heartbeat', ts_write_ms:long, seq:long})
 *   Memgraph:  (:Event:Heartbeat {date: 'YYYYMMDD' (string), subtype:'Heartbeat', ts_write_ms, seq})
 *   JanusGraph: Event vertex with {eventType:'Heartbeat', eventDate: long YYYYMMDD, ts_write_ms, seq}
 */
public final class HeartbeatService implements AutoCloseable {

    public static final class Config {
        public final boolean enabled;
        public final long    writeIntervalMs;   // default 2000
        public final long    readIntervalMs;    // default 2000
        public final YearMonth hbMonth;         // fixed window for HB (e.g., 2011-01)
        public final QueryGenerator.Engine engine;

        public Config(boolean enabled, long writeIntervalMs, long readIntervalMs,
                      YearMonth hbMonth, QueryGenerator.Engine engine) {
            this.enabled        = enabled;
            this.writeIntervalMs= writeIntervalMs;
            this.readIntervalMs = readIntervalMs;
            this.hbMonth        = hbMonth;
            this.engine         = engine;
        }
    }

    private final DatabaseClient db;
    private final Config cfg;

    private final ScheduledExecutorService sched =
            Executors.newScheduledThreadPool(2, r -> {
                Thread t = Thread.ofVirtual().name("hb-"+UUID.randomUUID()).factory().newThread(r);
                t.setDaemon(true);
                return t;
            });

    private final AtomicLong seq = new AtomicLong(0L);
    private final List<FreshnessSample> samples = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean running = false;

    // Pre-bound window (first-of-month .. first-of-next-month) as engine-typed values
    private final Object startDateParam;
    private final Object endDateParam;

    public HeartbeatService(DatabaseClient db, Config cfg) {
        this.db = db;
        this.cfg = cfg;

        LocalDate start = LocalDate.of(cfg.hbMonth.getYear(), cfg.hbMonth.getMonth(), 1);
        LocalDate end   = start.plusMonths(1);

        switch (cfg.engine) {
            case JANUSGRAPH -> {
                startDateParam = toYYYYMMDDLong(start);
                endDateParam   = toYYYYMMDDLong(end);
            }
            case NEO4J, MEMGRAPH -> {
                // Neo4j wants LocalDate; Memgraph dataset typically stored date as string.
                // We pass both typed and string per engine on use.
                startDateParam = start;
                endDateParam   = end;
            }
            default -> throw new IllegalArgumentException("Unknown engine: " + cfg.engine);
        }
    }

    /** Start writer and sampler if enabled. */
    public void start() {
        if (!cfg.enabled) return;
        running = true;

        sched.scheduleAtFixedRate(this::safeWriteBeat, 0, cfg.writeIntervalMs, MILLISECONDS);
        sched.scheduleAtFixedRate(this::safeSampleFreshness, cfg.readIntervalMs, cfg.readIntervalMs, MILLISECONDS);
    }

    /** Stop, flush, and print a percentile summary. */
    public void stopAndReport() {
        if (!cfg.enabled) return;
        running = false;
        sched.shutdown();
        try { sched.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}

        // CSV
        try (var rl = new com.benchmark.util.ResultsLogger("heartbeat_freshness.csv",
                "timestamp","seq","freshness_ms")) {
            synchronized (samples) {
                for (FreshnessSample s : samples) {
                    rl.log(Map.of(
                            "seq", s.seq(),
                            "freshness_ms", s.freshnessMs()
                    ));
                }
            }
        } catch (Exception e) {
            System.err.println("[HB] Failed to write heartbeat_freshness.csv: " + e.getMessage());
        }

        // Console percentiles
        var arr = samples.stream().mapToLong(FreshnessSample::freshnessMs).sorted().toArray();
        if (arr.length == 0) {
            System.out.println("[HB] No freshness samples collected.");
            return;
        }
        System.out.println("\n--- Freshness (Heartbeat) ---");
        System.out.printf("Samples: %,d (interval %d ms write / %d ms read)%n", arr.length, cfg.writeIntervalMs, cfg.readIntervalMs);
        System.out.printf("Latest:  %,d ms%n", arr[arr.length-1]);
        System.out.printf("p50:     %,d ms%n", pct(arr, 50));
        System.out.printf("p95:     %,d ms%n", pct(arr, 95));
        System.out.printf("p99:     %,d ms%n", pct(arr, 99));
        System.out.printf("max:     %,d ms%n", arr[arr.length-1]);
        System.out.println("----------------------------------------");
    }

    @Override public void close() { stopAndReport(); }

    /* ========================= internals ========================= */

    private void safeWriteBeat() {
        try {
            if (!running) return;
            writeBeat();
        } catch (Throwable t) {
            System.err.println("[HB][write] " + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
    }

    private void safeSampleFreshness() {
        try {
            if (!running) return;
            Long ts = readLatestWriteTimestamp();
            if (ts != null && ts > 0) {
                long now = System.currentTimeMillis();
                long fresh = Math.max(0, now - ts);
                long s = seq.get();
                samples.add(new FreshnessSample(now, fresh, s));
            }
        } catch (Throwable t) {
            System.err.println("[HB][read] " + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
    }

    private void writeBeat() {
        long idSeq = seq.incrementAndGet();
        long ts = System.currentTimeMillis();

        // Choose a day inside HB month to ensure it falls in the pre-bound window
        LocalDate day = LocalDate.of(cfg.hbMonth.getYear(), cfg.hbMonth.getMonthValue(), 1 + (int)(idSeq % 28));

        switch (cfg.engine) {
            case NEO4J -> {
                String cypher =
                        "CREATE (e:Event:Heartbeat {" +
                                " eventId:$id, subtype:'Heartbeat', date:$date, ts_write_ms:$ts, seq:$seq}) RETURN e";
                Map<String,Object> p = Map.of(
                        "id",  "hb_" + idSeq,
                        "date", day,            // Neo4j DATE type (driver handles LocalDate)
                        "ts",  ts,
                        "seq", idSeq
                );
                db.executeQuery(cypher, p);
            }

            case MEMGRAPH -> {
                // Memgraph dataset has date stored as string 'YYYYMMDD'
                String cypher =
                        "CREATE (e:Event:Heartbeat {" +
                                " eventId:$id, subtype:'Heartbeat', date:$date, ts_write_ms:$ts, seq:$seq}) RETURN e";
                Map<String,Object> p = Map.of(
                        "id",  "hb_" + idSeq,
                        "date", yyyymmddString(day),
                        "ts",  ts,
                        "seq", idSeq
                );
                db.executeQuery(cypher, p);
            }

            case JANUSGRAPH -> {
                // Gremlin: Event vertex with eventType + eventDate long
                String g =
                        "addV('Event')" +
                        ".property('eventId', id)" +
                        ".property('eventType','Heartbeat')" +
                        ".property('eventDate', evDate)" +
                        ".property('ts_write_ms', ts)" +
                        ".property('seq', seq)" +
                        ".iterate()";
                Map<String,Object> b = new HashMap<>();
                b.put("id", "hb_" + idSeq);
                b.put("evDate", toYYYYMMDDLong(day));
                b.put("ts", ts);
                b.put("seq", idSeq);
                db.executeQuery(g, b);
            }
        }
    }

    private Long readLatestWriteTimestamp() {
        switch (cfg.engine) {
            case NEO4J -> {
                String cypher =
                        "MATCH (e:Event:Heartbeat) " +
                        "WHERE e.date >= $start AND e.date < $end " +
                        "RETURN e.ts_write_ms AS ts, e.seq AS seq " +
                        "ORDER BY e.seq DESC LIMIT 1";
                Map<String,Object> p = Map.of("start", startDateParam, "end", endDateParam);
                List<Map<String,Object>> rows = db.executeQuery(cypher, p);
                if (!rows.isEmpty()) {
                    Object ts = rows.get(0).get("ts");
                    return (ts instanceof Number n) ? n.longValue() : null;
                }
                return null;
            }
            case MEMGRAPH -> {
                String cypher =
                        "MATCH (e:Event:Heartbeat) " +
                        "WHERE e.date >= $start AND e.date < $end " +
                        "RETURN e.ts_write_ms AS ts, e.seq AS seq " +
                        "ORDER BY e.seq DESC LIMIT 1";
                Map<String,Object> p = Map.of(
                        "start", yyyymmddString((LocalDate) startDateParam),
                        "end",   yyyymmddString((LocalDate) endDateParam)
                );
                List<Map<String,Object>> rows = db.executeQuery(cypher, p);
                if (!rows.isEmpty()) {
                    Object ts = rows.get(0).get("ts");
                    return (ts instanceof Number n) ? n.longValue() : null;
                }
                return null;
            }
            case JANUSGRAPH -> {
                String g =
                        "g.V().hasLabel('Event')" +
                        ".has('eventType','Heartbeat')" +
                        ".has('eventDate', gte(startDate))" +
                        ".has('eventDate', lt(endDate))" +
                        ".order().by('seq', decr)" +
                        ".limit(1)" +
                        ".project('ts','seq')" +
                        ".by(values('ts_write_ms'))" +
                        ".by(values('seq'))";
                Map<String,Object> b = Map.of(
                        "startDate", startDateParam,
                        "endDate",   endDateParam
                );
                List<Map<String,Object>> rows = db.executeQuery(g, b);
                if (!rows.isEmpty()) {
                    Object ts = rows.get(0).get("ts");
                    return (ts instanceof Number n) ? n.longValue() : null;
                }
                return null;
            }
            default -> throw new IllegalArgumentException("Unknown engine: " + cfg.engine);
        }
    }

    private static long toYYYYMMDDLong(LocalDate d) {
        return d.getYear() * 10000L + d.getMonthValue() * 100L + d.getDayOfMonth();
    }

    private static String yyyymmddString(LocalDate d) {
        return "%04d%02d%02d".formatted(d.getYear(), d.getMonthValue(), d.getDayOfMonth());
    }

    private static long pct(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        double rank = (p/100.0)*(sorted.length-1);
        int lo = (int)Math.floor(rank);
        int hi = (int)Math.ceil(rank);
        if (lo == hi) return sorted[lo];
        double w = rank - lo;
        return (long)Math.round(sorted[lo] * (1.0 - w) + sorted[hi] * w);
    }
}
