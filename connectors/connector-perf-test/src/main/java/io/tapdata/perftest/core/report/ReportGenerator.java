package io.tapdata.perftest.core.report;

import java.io.IOException;

public interface ReportGenerator {
    void generate(BenchmarkReport report) throws IOException;
}
