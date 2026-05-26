package io.tapdata.perftest.core.report;

import io.tapdata.perftest.core.scenario.ScenarioResult;

import java.time.LocalDateTime;
import java.util.List;

public class BenchmarkReport {

    private final String             title;
    private final List<ScenarioResult> results;
    private final LocalDateTime      generatedAt = LocalDateTime.now();

    public BenchmarkReport(String title, List<ScenarioResult> results) {
        this.title   = title;
        this.results = results;
    }

    public String               getTitle()       { return title; }
    public List<ScenarioResult> getResults()     { return results; }
    public LocalDateTime        getGeneratedAt() { return generatedAt; }
}
