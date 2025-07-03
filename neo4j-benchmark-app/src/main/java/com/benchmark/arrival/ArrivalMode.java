package com.benchmark.arrival;

/** Closed-loop (today’s behaviour) or Open-loop (Poisson arrival model). */
public enum ArrivalMode {
    CLOSED, OPEN;

    public static ArrivalMode fromString(String s) {
        return "open".equalsIgnoreCase(s) ? OPEN : CLOSED;
    }
}
