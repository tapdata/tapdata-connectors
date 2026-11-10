package io.tapdata.connector.postgres;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.AfterAll;
import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.it.verifier.ConnectorVerifier;

import java.util.Objects;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/** PostgreSQL CDC IT against DBForge's EPAS EFM cluster with replica WAL and TDE encryption. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(PostgresEfmReadOnlyCondition.class)
public class PostgresEfmTdeConnectorIT extends PostgresConnectorIT {

    private HikariDataSource primaryVerifierDataSource;

    @Override
    protected ConnectorVerifier createVerifier() {
        if (!io.tapdata.it.dbforge.DbForgeLeaseProvider.isDbForgeSelected()) {
            return super.createVerifier();
        }
        String leaseId = Objects.requireNonNull(dbForgeLeaseProvider, "DBForge lease is not acquired").getLeaseId();
        String namespace = dbForgeEphemeralNamespace();
        String primaryHost = leaseId + "-0." + leaseId + "-hs." + namespace + ".svc.cluster.local";
        if (primaryVerifierDataSource == null) {
            primaryVerifierDataSource = new HikariDataSource();
            primaryVerifierDataSource.setJdbcUrl("jdbc:postgresql://" + primaryHost + ":5432/"
                    + dbForgeConnectionConfig.getString("database"));
            primaryVerifierDataSource.setUsername(dbForgeConnectionConfig.getString("user"));
            primaryVerifierDataSource.setPassword(dbForgeConnectionConfig.getString("password"));
            primaryVerifierDataSource.setMaximumPoolSize(2);
            primaryVerifierDataSource.setMinimumIdle(0);
            primaryVerifierDataSource.setConnectionTimeout(10_000);
        }
        return new PostgresJdbcVerifier(primaryVerifierDataSource, dbForgeConnectionConfig.getString("schema"));
    }

    @Override
    protected String dbForgeTopology() {
        return "efm-tde";
    }

    @Override
    protected String defaultCdcMode() {
        return "postgres-physical";
    }

    @Override
    protected Object sourceDdlStreamOffset() throws Throwable {
        try (Connection connection = primaryVerifierDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // Standby pg_control_checkpoint() reports the last restartpoint, which
            // can still point into recycled WAL immediately after DbForge startup.
            // Use the shortest supported interval, force a primary checkpoint, and
            // wait until the standby has persisted a restartpoint at that checkpoint
            // before starting the physical miner's page-cache warmup.
            statement.execute("ALTER SYSTEM SET checkpoint_timeout = '30s'");
            statement.execute("SELECT pg_reload_conf()");
            configureStandbyCheckpointInterval();
            statement.execute("CHECKPOINT");
            try (ResultSet result = statement.executeQuery(
                    "SELECT pg_current_wal_flush_lsn(), redo_lsn FROM pg_control_checkpoint()")) {
                if (!result.next()) {
                    throw new IllegalStateException("Primary PostgreSQL did not return the current flushed WAL LSN");
                }
                String offset = result.getString(1);
                String checkpointRedoLsn = result.getString(2);
                awaitStandbyReplay(offset, checkpointRedoLsn);
                return offset;
            }
        }
    }

    private void configureStandbyCheckpointInterval() throws Exception {
        String standbyUrl = standbyJdbcUrl();
        try (Connection connection = java.sql.DriverManager.getConnection(standbyUrl,
                dbForgeConnectionConfig.getString("user"), dbForgeConnectionConfig.getString("password"));
             Statement statement = connection.createStatement()) {
            statement.execute("ALTER SYSTEM SET checkpoint_timeout = '30s'");
            statement.execute("SELECT pg_reload_conf()");
        }
    }

    private void awaitStandbyReplay(String targetLsn, String targetRedoLsn) throws Exception {
        String standbyUrl = standbyJdbcUrl();
        long target = org.postgresql.replication.LogSequenceNumber.valueOf(targetLsn).asLong();
        long targetRedo = org.postgresql.replication.LogSequenceNumber.valueOf(targetRedoLsn).asLong();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try (Connection connection = java.sql.DriverManager.getConnection(standbyUrl,
                    dbForgeConnectionConfig.getString("user"), dbForgeConnectionConfig.getString("password"));
                 Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT pg_last_wal_replay_lsn(), redo_lsn FROM pg_control_checkpoint()")) {
                if (result.next() && result.getString(1) != null
                        && org.postgresql.replication.LogSequenceNumber.valueOf(result.getString(1)).asLong() >= target
                        && result.getString(2) != null
                        && org.postgresql.replication.LogSequenceNumber.valueOf(result.getString(2)).asLong() >= targetRedo) {
                    return;
                }
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("DBForge PostgreSQL standby did not replay and persist checkpoint redo "
                + targetRedoLsn + " through test offset " + targetLsn);
    }

    private String standbyJdbcUrl() {
        return "jdbc:postgresql://" + dbForgeConnectionConfig.getString("host") + ":"
                + dbForgeConnectionConfig.getInteger("port") + "/" + dbForgeConnectionConfig.getString("database");
    }

    @AfterAll
    void closePrimaryVerifierDataSource() {
        if (primaryVerifierDataSource != null) {
            primaryVerifierDataSource.close();
        }
    }
}
