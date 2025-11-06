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
 * - Writes tiny "heartbeat" Events at a fixed rate using SAME label/props the OLAP range uses.
 * - Samples the most recent visible heartbeat to compute:
 *      freshness = now_ms - ts_write_ms_of_latest_visible_heartbeat
 * - Prints effective config + inter-write/read deltas for verification.
 *
 * Neo4j:     (:Event:Heartbeat {date: LocalDate, subtype:'Heartbeat', ts_write_ms:long, seq:long})
 * Memgraph:  (:Event:Heartbeat {date: 'YYYYMMDD', subtype:'Heartbeat', ts_write_ms, seq})
 * JanusGraph: Event vertex with {eventType:'Heartbeat', eventDate: YYYYMMDD(long), ts_write_ms, seq}
 */
public final class HeartbeatService implements AutoCloseable {

    public static final class Config {
        public final boolean enabled;
        public final long    writeIntervalMs;
        public final long    readIntervalMs;
        public final YearMonth hbMonth;
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

    // window bounds as engine-typed values
    private final Object startDateParam;
    private final Object endDateParam;

    // verification helpers
    private volatile long lastWriteWall = -1L;
    private volatile long lastSampleWall = -1L;
    private final List<Long> writeDeltas = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> readDeltas  = Collections.synchronizedList(new ArrayList<>());

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

        System.out.printf("[HB] start: write=%d ms, read=%d ms, window=%s%n",
                cfg.writeIntervalMs, cfg.readIntervalMs, cfg.hbMonth);

        sched.scheduleAtFixedRate(this::safeWriteBeat, 0, cfg.writeIntervalMs, MILLISECONDS);
        sched.scheduleAtFixedRate(this::safeSampleFreshness, cfg.readIntervalMs, cfg.readIntervalMs, MILLISECONDS);
    }

    /** Stop, flush, print percentiles + delta verification. */
    public void stopAndReport() {
        if (!cfg.enabled) return;
        running = false;
        sched.shutdown();
        try { sched.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}

        // Console percentiles
        var arr = samples.stream().mapToLong(FreshnessSample::freshnessMs).sorted().toArray();
        if (arr.length == 0) {
            System.out.println("[HB] No freshness samples collected.");
        } else {
            System.out.println("\n--- Freshness (Heartbeat) ---");
            System.out.printf("Samples: %,d (interval %d ms write / %d ms read)%n",
                    arr.length, cfg.writeIntervalMs, cfg.readIntervalMs);
            System.out.printf("Latest:  %,d ms%n", arr[arr.length-1]);
            System.out.printf("p50:     %,d ms%n", pct(arr, 50));
            System.out.printf("p95:     %,d ms%n", pct(arr, 95));
            System.out.printf("p99:     %,d ms%n", pct(arr, 99));
            System.out.printf("max:     %,d ms%n", arr[arr.length-1]);
            System.out.println("----------------------------------------");
        }

        if (!writeDeltas.isEmpty()) {
            long[] w = writeDeltas.stream().mapToLong(Long::longValue).sorted().toArray();
            System.out.printf("[HB] write Δms p50=%d p95=%d p99=%d (n=%d)%n",
                    pct(w,50), pct(w,95), pct(w,99), w.length);
        }
        if (!readDeltas.isEmpty()) {
            long[] r = readDeltas.stream().mapToLong(Long::longValue).sorted().toArray();
            System.out.printf("[HB] read  Δms p50=%d p95=%d p99=%d (n=%d)%n",
                    pct(r,50), pct(r,95), pct(r,99), r.length);
        }
    }

    @Override public void close() { stopAndReport(); }

    /* ========================= internals ========================= */

    private void safeWriteBeat() {
        try {
            if (!running) return;
            long now = System.currentTimeMillis();
            if (lastWriteWall > 0) writeDeltas.add(now - lastWriteWall);
            lastWriteWall = now;
            writeBeat(now);
        } catch (Throwable t) {
            System.err.println("[HB][write] " + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
    }

    private void safeSampleFreshness() {
        try {
            if (!running) return;
            long now = System.currentTimeMillis();
            if (lastSampleWall > 0) readDeltas.add(now - lastSampleWall);
            lastSampleWall = now;

            Long ts = readLatestWriteTimestamp();
            if (ts != null && ts > 0) {
                long fresh = Math.max(0, now - ts);
                long s = seq.get();
                samples.add(new FreshnessSample(now, fresh, s));
            }
        } catch (Throwable t) {
            System.err.println("[HB][read] " + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
    }

    private void writeBeat(long now) {
        long idSeq = seq.incrementAndGet();
        long ts = now;

        // choose a day inside HB month → falls within pre-bound window
        LocalDate day = LocalDate.of(cfg.hbMonth.getYear(), cfg.hbMonth.getMonthValue(), 1 + (int)(idSeq % 28));

        switch (cfg.engine) {
            case NEO4J -> {
                String cypher =
                        "CREATE (e:Event:Heartbeat {" +
                                " eventId:$id, subtype:'Heartbeat', date:$date, ts_write_ms:$ts, seq:$seq}) RETURN e";
                Map<String,Object> p = Map.of(
                        "id",  "hb_" + idSeq,
                        "date", day,
                        "ts",  ts,
                        "seq", idSeq
                );
                db.executeQuery(cypher, p);
            }

            case MEMGRAPH -> {
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
                        ".order().by('seq', desc)" +
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

    /* ========================= helpers ========================= */

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

    /** Small record to hold samples. */
    public record FreshnessSample(long wallClockMs, long freshnessMs, long seq) {}
}
