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
 * Heartbeat writer/sampler (freshness).
 * Neo4j & Memgraph use date as String "yyyyMMdd".
 * Janus/Gremlin uses long yyyymmdd (eventDate).
 */
public final class HeartbeatService implements AutoCloseable {

    public static final class Config {
        public final boolean enabled;
        public final long writeIntervalMs;
        public final long readIntervalMs;
        public final YearMonth hbMonth;
        public final QueryGenerator.Engine engine;

        public Config(boolean enabled, long writeIntervalMs, long readIntervalMs,
                      YearMonth hbMonth, QueryGenerator.Engine engine) {
            this.enabled = enabled;
            this.writeIntervalMs = writeIntervalMs;
            this.readIntervalMs = readIntervalMs;
            this.hbMonth = hbMonth;
            this.engine = engine;
        }
    }

    private final DatabaseClient db;
    private final Config cfg;

    private final ScheduledExecutorService sched =
            Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "hb-sched-" + UUID.randomUUID());
                t.setDaemon(true);
                return t;
            });

    private final AtomicLong seq = new AtomicLong(0L);
    private final List<FreshnessSample> samples = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean running = false;

    private final Object startParam; // String yyyyMMdd (Neo4j/Memgraph) or long yyyymmdd (Janus)
    private final Object endParam;

    private volatile long lastWriteWall = -1L;
    private volatile long lastSampleWall = -1L;
    private final List<Long> writeDeltas = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> readDeltas  = Collections.synchronizedList(new ArrayList<>());

    private final String hbRunPrefix;
    private final String hbRunRand;

    public HeartbeatService(DatabaseClient db, Config cfg) {
        this.db = db;
        this.cfg = cfg;

        LocalDate start = LocalDate.of(cfg.hbMonth.getYear(), cfg.hbMonth.getMonth(), 1);
        LocalDate end   = start.plusMonths(1);

        if (cfg.engine == QueryGenerator.Engine.JANUSGRAPH) {
            startParam = toYYYYMMDDLong(start);
            endParam   = toYYYYMMDDLong(end);
        } else {
            startParam = yyyymmdd(start);
            endParam   = yyyymmdd(end);
        }

        long runStart = System.currentTimeMillis();
        hbRunPrefix = String.format("%tY%<tm%<tdT%<tH%<tM%<tS%<tLZ", runStart);
        hbRunRand   = Long.toString(ThreadLocalRandom.current().nextLong(1_000_000_000L), 36).toUpperCase();
    }

    public void start() {
        if (!cfg.enabled) return;
        running = true;

        System.out.printf("[HB] start: write=%d ms, read=%d ms, window=%s%n",
                cfg.writeIntervalMs, cfg.readIntervalMs, cfg.hbMonth);

        sched.scheduleAtFixedRate(this::safeWriteBeat, 0, cfg.writeIntervalMs, MILLISECONDS);
        sched.scheduleAtFixedRate(this::safeSampleFreshness, cfg.readIntervalMs, cfg.readIntervalMs, MILLISECONDS);
    }

    public void stopAndReport() {
        if (!cfg.enabled) return;
        running = false;
        sched.shutdown();
        try { sched.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}

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

    /* internals */

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
        long n = seq.incrementAndGet();
        String id = "hb_" + hbRunPrefix + "_" + now + "_" + hbRunRand + "_" + n;

        LocalDate day = LocalDate.of(cfg.hbMonth.getYear(), cfg.hbMonth.getMonthValue(), 1 + (int)(n % 28));

        switch (cfg.engine) {
            case NEO4J, MEMGRAPH -> {
                String cypher = "CREATE (e:Event:Heartbeat {eventId:$id, date:$date, ts_write_ms:$ts, subtype:'Heartbeat', seq:$seq}) RETURN e";
                Map<String,Object> p = Map.of("id", id, "date", yyyymmdd(day), "ts", now, "seq", n);
                db.executeQuery(cypher, p);
            }
            case JANUSGRAPH -> {
                String g = "g.addV('Event').property('eventId', eventId).property('eventType','Heartbeat').property('eventDate', evDate).property('ts_write_ms', ts).property('seq', seq).iterate()";
                Map<String,Object> b = new HashMap<>();
                b.put("eventId", id);
                b.put("evDate", toYYYYMMDDLong(day));
                b.put("ts", now);
                b.put("seq", n);
                db.executeQuery(g, b);
            }
        }
    }

    private Long readLatestWriteTimestamp() {
        switch (cfg.engine) {
            case NEO4J, MEMGRAPH -> {
                String cypher = "MATCH (e:Event:Heartbeat) WHERE e.date >= $start AND e.date < $end RETURN max(e.ts_write_ms) AS ts";
                List<Map<String,Object>> rows = db.executeQuery(cypher, Map.of("start", startParam, "end", endParam));
                if (!rows.isEmpty()) {
                    Object v = rows.get(0).get("ts");
                    return (v instanceof Number n) ? n.longValue() : null;
                }
                return null;
            }
            case JANUSGRAPH -> {
                String g = "g.V().hasLabel('Event').has('eventType','Heartbeat').has('eventDate', gte(start)).has('eventDate', lt(end)).values('ts_write_ms').max()";
                List<Map<String,Object>> rows = db.executeQuery(g, Map.of("start", startParam, "end", endParam));
                if (!rows.isEmpty()) {
                    Object v = rows.get(0).get("value");
                    return (v instanceof Number n) ? n.longValue() : null;
                }
                return null;
            }
            default -> throw new IllegalArgumentException("engine");
        }
    }

    private static long toYYYYMMDDLong(LocalDate d) {
        return d.getYear()*10000L + d.getMonthValue()*100L + d.getDayOfMonth();
    }

    private static String yyyymmdd(LocalDate d) {
        return String.format(Locale.ROOT, "%04d%02d%02d", d.getYear(), d.getMonthValue(), d.getDayOfMonth());
    }

    private static long pct(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        double rank = (p/100.0)*(sorted.length-1);
        int lo = (int)Math.floor(rank), hi = (int)Math.ceil(rank);
        if (lo == hi) return sorted[lo];
        double w = rank - lo;
        return Math.round(sorted[lo]*(1.0-w) + sorted[hi]*w);
    }

    public record FreshnessSample(long wallClockMs, long freshnessMs, long seq) {}
}

