package com.benchmark.arrival;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;

/** Generates Poisson-scheduled tokens until {@code endTimeMillis}. */
public final class ArrivalTimer implements Runnable {

    private static final Object TOKEN = new Object();

    private final ArrivalScheduler scheduler;
    private final BlockingQueue<Object> tokenQueue;
    private final long endTimeMillis;

    public ArrivalTimer(double lambdaPerSecond,
                        BlockingQueue<Object> tokenQueue,
                        long endTimeMillis) {
        this.scheduler     = new ArrivalScheduler(lambdaPerSecond);
        this.tokenQueue    = tokenQueue;
        this.endTimeMillis = endTimeMillis;
    }

    @Override
    public void run() {
        while (System.currentTimeMillis() < endTimeMillis &&
               !Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(scheduler.nextDelayNanos());
            tokenQueue.offer(TOKEN);                 // never blocks (unbounded queue)
        }
    }
}
