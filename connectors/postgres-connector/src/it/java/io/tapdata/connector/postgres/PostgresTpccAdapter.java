package io.tapdata.connector.postgres;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

final class PostgresTpccAdapter implements TpccAdapter {
    private static final List<String> TABLES = Arrays.asList("bmsql_config", "bmsql_warehouse", "bmsql_district",
            "bmsql_customer", "bmsql_history", "bmsql_new_order", "bmsql_oorder", "bmsql_order_line", "bmsql_item", "bmsql_stock");
    private final DataMap config;
    private final Path home;
    private final Path runDirectory;
    private final Path workDirectory;
    private final Path propertiesFile;
    private final Path logFile;
    private TpccConfig tpccConfig;

    PostgresTpccAdapter(DataMap config) {
        this.config = config;
        home = Paths.get(value("tpcc.benchmarkHome", "TPCC_BENCHMARK_HOME", "/home/t-layer3-test/auto_test/benchmarksql"));
        runDirectory = home.resolve("run");
        workDirectory = Paths.get(value("tpcc.workDir", "TPCC_WORK_DIR", "target/tpcc-postgres")).toAbsolutePath();
        propertiesFile = workDirectory.resolve("postgres.properties");
        logFile = workDirectory.resolve("benchmarksql.log");
    }

    public List<String> tableNames() { return TABLES; }
    public boolean isPrepared() throws SQLException { try (Connection connection = open(); Statement statement = connection.createStatement()) { for (String table : TABLES) try (ResultSet rows = statement.executeQuery("SELECT 1 FROM " + q(table) + " LIMIT 1")) { if (!rows.next()) return false; } catch (SQLException error) { return false; } return true; } }
    public void prepare(TpccConfig value) throws Exception { tpccConfig = value; validate(); Files.createDirectories(workDirectory); write(value); run("runDatabaseDestroy.sh", true); run("runDatabaseBuild.sh", false); if (!isPrepared()) throw new IllegalStateException("PostgreSQL TPCC preparation failed"); }
    public Map<String, Long> currentRowCounts() throws SQLException { Map<String, Long> result = new LinkedHashMap<>(); try (Connection connection = open(); Statement statement = connection.createStatement()) { for (String table : TABLES) try (ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + q(table))) { rows.next(); result.put(table, rows.getLong(1)); } } return result; }
    public void runWorkload(TpccConfig value) throws Exception { tpccConfig = value; write(value); run("runBenchmark.sh", false); }
    public void verifyConsistency() throws SQLException { zero("orphan orders", "SELECT COUNT(*) FROM " + q("bmsql_oorder") + " o LEFT JOIN " + q("bmsql_customer") + " c ON c.c_w_id=o.o_w_id AND c.c_d_id=o.o_d_id AND c.c_id=o.o_c_id WHERE c.c_id IS NULL"); zero("orphan lines", "SELECT COUNT(*) FROM " + q("bmsql_order_line") + " l LEFT JOIN " + q("bmsql_oorder") + " o ON o.o_w_id=l.ol_w_id AND o.o_d_id=l.ol_d_id AND o.o_id=l.ol_o_id WHERE o.o_id IS NULL"); }
    public void cleanup() throws Exception { if (Files.isRegularFile(propertiesFile)) run("runDatabaseDestroy.sh", true); }

    private void validate() {
        List<Path> required = Arrays.asList(runDirectory.resolve("runDatabaseBuild.sh"), runDirectory.resolve("runDatabaseDestroy.sh"), runDirectory.resolve("runBenchmark.sh"), home.resolve("dist/BenchmarkSQL-5.0.jar"), home.resolve("lib/postgres/postgresql-42.7.7.jar"));
        List<Path> missing = required.stream().filter(path -> !Files.isRegularFile(path)).collect(Collectors.toList());
        if (!missing.isEmpty()) throw new IllegalStateException("TPCC assets missing: " + missing);
    }

    private void write(TpccConfig value) throws IOException {
        String text = "db=postgres\ndriver=org.postgresql.Driver\nconn=" + url() + "\nuser=" + config.getString("user") + "\npassword=" + config.getString("password")
                + "\n\nwarehouses=" + value.getWarehouses() + "\nloadWorkers=" + value.getLoadWorkers() + "\nterminals=" + value.getTerminals()
                + "\nrunTxnsPerTerminal=" + value.getTransactionsPerTerminal() + "\nrunMins=" + value.getRunMinutes()
                + "\nlimitTxnsPerMin=3000000\nterminalWarehouseFixed=false\nnewOrderWeight=45\npaymentWeight=43\norderStatusWeight=4\ndeliveryWeight=4\nstockLevelWeight=4\n";
        Files.write(propertiesFile, text.getBytes(StandardCharsets.UTF_8));
    }

    private void run(String script, boolean tolerateFailure) throws Exception {
        ProcessBuilder builder = new ProcessBuilder("bash", runDirectory.resolve(script).toString(), propertiesFile.toString());
        builder.directory(runDirectory.toFile()).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
        Process process = builder.start();
        int timeout = tpccConfig == null ? 600 : tpccConfig.getTimeoutSeconds();
        if (!process.waitFor(timeout, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new IllegalStateException(script + " timed out"); }
        if (process.exitValue() != 0 && !tolerateFailure) throw new IllegalStateException(script + " failed, see " + logFile);
    }

    private void zero(String name, String sql) throws SQLException { try (Connection connection = open(); Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) { if (!result.next() || result.getLong(1) != 0) throw new AssertionError(name); } }
    private Connection open() throws SQLException { return DriverManager.getConnection(url(), config.getString("user"), config.getString("password")); }
    private String url() { return "jdbc:postgresql://" + config.getString("host") + ":" + config.getInteger("port") + "/" + config.getString("database") + "?currentSchema=" + config.getString("schema"); }
    private String q(String table) { return "\"" + config.getString("schema") + "\".\"" + table + "\""; }
    private static String value(String property, String environment, String fallback) { String result = System.getProperty(property); if (result == null || result.trim().isEmpty()) result = System.getenv(environment); return result == null || result.trim().isEmpty() ? fallback : result.trim(); }
}
