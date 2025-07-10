package com.benchmark.util;

import org.apache.commons.csv.*;
import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.List;        // ← ADD
import java.util.ArrayList;   // ← ADD
import java.util.Map;

/**
 * Lightweight CSV logger: appends one row per benchmark sample.
 */
public final class ResultsLogger implements Closeable {

    private final CSVPrinter csv;
    private final Path path;
    private final String[] headers;   // already declared

    public ResultsLogger(String fileName, String... headers) throws IOException {
        this.path = Paths.get(fileName).toAbsolutePath();
        this.headers = headers;                   // ← INITIALISE
        boolean freshFile = Files.notExists(path);

        BufferedWriter writer = Files.newBufferedWriter(
                path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        csv = new CSVPrinter(writer, CSVFormat.DEFAULT);
        if (freshFile) {
            csv.printRecord((Object[]) headers);  // write header exactly once
        }
    }

    public void log(Map<String, Object> data) throws IOException {
    List<Object> row = new ArrayList<>();
    row.add(Instant.now().toString());

    for (String h : headers) {
        if ("timestamp".equals(h)) continue;
        row.add(data.getOrDefault(h, ""));
    }
    csv.printRecord(row);
    csv.flush();
}
    @Override public void close() throws IOException { csv.close(); }

    public Path getPath() { return path; }
}
