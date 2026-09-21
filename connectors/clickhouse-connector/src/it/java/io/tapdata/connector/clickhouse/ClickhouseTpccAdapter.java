package io.tapdata.connector.clickhouse;

import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConfig;
import org.junit.jupiter.api.Assumptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class ClickhouseTpccAdapter implements TpccAdapter {
    private static final List<String> TABLES = Arrays.asList("bmsql_config", "bmsql_warehouse", "bmsql_district",
            "bmsql_customer", "bmsql_history", "bmsql_new_order", "bmsql_oorder", "bmsql_order_line", "bmsql_item", "bmsql_stock");

    public List<String> tableNames() { return TABLES; }
    public boolean isPrepared() { return false; }
    public void prepare(TpccConfig config) { Assumptions.assumeTrue(false, "BenchmarkSQL does not support ClickHouse transactional TPCC workloads"); }
    public Map<String, Long> currentRowCounts() { return Collections.emptyMap(); }
    public void runWorkload(TpccConfig config) { Assumptions.assumeTrue(false, "ClickHouse does not expose the transactional TPCC API"); }
    public void verifyConsistency() { }
    public void cleanup() { }
}
