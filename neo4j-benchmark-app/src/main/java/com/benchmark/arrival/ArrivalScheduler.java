package com.benchmark.arrival;

import java.util.concurrent.ThreadLocalRandom;

/** Generates exponential inter-arrival delays for a Poisson(λ) process. */
public final class ArrivalScheduler {
    private final double lambdaPerNs;                       // arrivals/ns
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    /** @param lambdaPerSec  arrival rate λ in queries/second (must be > 0) */
    public ArrivalScheduler(double lambdaPerSec) {
        if (lambdaPerSec <= 0)
            throw new IllegalArgumentException("λ must be > 0");
        this.lambdaPerNs = lambdaPerSec / 1_000_000_000.0;
    }

    /** Next delay in nanoseconds. */
    public long nextDelayNanos() {
        double u = rnd.nextDouble();               // (0,1)
        return (long) (-Math.log(1.0 - u) / lambdaPerNs);
    }
}
