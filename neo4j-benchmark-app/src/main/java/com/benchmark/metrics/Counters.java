package com.benchmark.metrics;

import java.util.concurrent.atomic.LongAdder;

/** Simple offered/completed/failed counters for visibility in OPEN mode. */
public final class Counters {
    public final LongAdder submitted = new LongAdder();
    public final LongAdder completed = new LongAdder();
    public final LongAdder failed    = new LongAdder();
}
