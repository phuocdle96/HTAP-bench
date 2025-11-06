package com.benchmark.freshness;

public record FreshnessSample(long wallClockMs, long freshnessMs, long seq) {}
