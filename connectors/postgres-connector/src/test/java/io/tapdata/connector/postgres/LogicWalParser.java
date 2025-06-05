package io.tapdata.connector.postgres;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * PostgreSQL WAL Parser for CDC data listening
 * Fixed version that properly handles logical replication and CDC events
 */
public class LogicWalParser {

    private static final Logger LOGGER = Logger.getLogger(LogicWalParser.class.getName());
    private static final String SLOT_NAME = "tapdata_cdc_test_slot";
    private static final String PREFERRED_PLUGIN = "wal2json"; // 首选插件
    private static final String FALLBACK_PLUGIN = "test_decoding"; // 备用插件

    private volatile boolean running = true;
    private PGReplicationStream stream;
    private String actualPlugin = PREFERRED_PLUGIN; // 实际使用的插件

    public static void main(String[] args) {
        LogicWalParser parser = new LogicWalParser();

        // 添加优雅关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down WAL parser...");
            parser.stop();
        }));

        parser.start();
    }

    public void start() {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";

        // 首先使用普通连接创建复制槽
        Properties normalProps = new Properties();
        normalProps.setProperty("user", "postgres");
        normalProps.setProperty("password", "gj0628");

        try {
            // 使用普通连接检查并创建复制槽
            try (Connection normalConnection = DriverManager.getConnection(jdbcUrl, normalProps)) {
                ensureReplicationSlot(normalConnection);
            }

            // 然后使用复制连接进行流式读取
            Properties replicationProps = new Properties();
            replicationProps.setProperty("user", "postgres");
            replicationProps.setProperty("password", "gj0628");
            replicationProps.setProperty("replication", "database"); // 关键：启用复制协议
            replicationProps.setProperty("assumeMinServerVersion", "9.4"); // 确保支持逻辑复制
            replicationProps.setProperty("preferQueryMode", "simple"); // 使用简单查询模式

            try (Connection replicationConnection = DriverManager.getConnection(jdbcUrl, replicationProps)) {
                PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);

                // 创建逻辑复制流
                stream = createReplicationStream(pgConnection);

                // 持续处理WAL记录
                processWALRecords(stream);
            }

        } catch (SQLException e) {
            LOGGER.severe("Failed to start WAL parser: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    public void stop() {
        running = false;
        if (stream != null) {
            try {
                stream.close();
            } catch (SQLException e) {
                LOGGER.warning("Error closing replication stream: " + e.getMessage());
            }
        }
    }

    /**
     * 确保复制槽存在，如果不存在则创建
     * 注意：必须使用普通连接，不能使用复制连接
     */
    private void ensureReplicationSlot(Connection connection) throws SQLException {
        LOGGER.info("Checking replication slot: " + SLOT_NAME);

        try (Statement stmt = connection.createStatement()) {
            // 检查复制槽是否存在
            String checkSlotSql = "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '" + SLOT_NAME + "'";
            boolean slotExists = false;

            try (ResultSet rs = stmt.executeQuery(checkSlotSql)) {
                if (rs.next()) {
                    slotExists = rs.getInt(1) > 0;
                }
            }

            if (!slotExists) {
                // 复制槽不存在，创建新的
                actualPlugin = determineAvailablePlugin(stmt);
                LOGGER.info("Creating replication slot: " + SLOT_NAME + " with plugin: " + actualPlugin);

                String createSlotSql = "SELECT pg_create_logical_replication_slot('" + SLOT_NAME + "', '" + actualPlugin + "')";
                try {
                    stmt.execute(createSlotSql);
                    LOGGER.info("Replication slot created successfully with plugin: " + actualPlugin);
                } catch (SQLException e) {
                    if (actualPlugin.equals(PREFERRED_PLUGIN)) {
                        // 如果首选插件失败，尝试备用插件
                        LOGGER.warning("Failed to create slot with " + PREFERRED_PLUGIN + ", trying " + FALLBACK_PLUGIN);
                        actualPlugin = FALLBACK_PLUGIN;
                        createSlotSql = "SELECT pg_create_logical_replication_slot('" + SLOT_NAME + "', '" + actualPlugin + "')";
                        stmt.execute(createSlotSql);
                        LOGGER.info("Replication slot created successfully with fallback plugin: " + actualPlugin);
                    } else {
                        throw e;
                    }
                }
            } else {
                LOGGER.info("Using existing replication slot: " + SLOT_NAME);
                // 检查现有槽使用的插件
                actualPlugin = getExistingSlotPlugin(stmt);
            }
        }
    }

    /**
     * 确定可用的插件
     */
    private String determineAvailablePlugin(Statement stmt) throws SQLException {
        // 首先尝试首选插件
        if (isPluginAvailable(stmt, PREFERRED_PLUGIN)) {
            LOGGER.info("Using preferred plugin: " + PREFERRED_PLUGIN);
            return PREFERRED_PLUGIN;
        }

        // 如果首选插件不可用，尝试备用插件
        if (isPluginAvailable(stmt, FALLBACK_PLUGIN)) {
            LOGGER.info("Using fallback plugin: " + FALLBACK_PLUGIN);
            return FALLBACK_PLUGIN;
        }

        // 如果都不可用，默认使用test_decoding（通常内置）
        LOGGER.warning("Neither preferred nor fallback plugin available, using test_decoding");
        return FALLBACK_PLUGIN;
    }

    /**
     * 检查特定插件是否可用
     */
    private boolean isPluginAvailable(Statement stmt, String pluginName) {
        try {
            // 检查插件是否在可用扩展列表中
            String checkPluginSql = "SELECT 1 FROM pg_available_extensions WHERE name = '" + pluginName + "'";
            try (ResultSet rs = stmt.executeQuery(checkPluginSql)) {
                return rs.next();
            }
        } catch (SQLException e) {
            LOGGER.warning("Could not check plugin availability for " + pluginName + ": " + e.getMessage());
            // 对于test_decoding，即使检查失败也认为可用（通常是内置的）
            return FALLBACK_PLUGIN.equals(pluginName);
        }
    }

    /**
     * 获取现有复制槽使用的插件
     */
    private String getExistingSlotPlugin(Statement stmt) throws SQLException {
        String getPluginSql = "SELECT plugin FROM pg_replication_slots WHERE slot_name = '" + SLOT_NAME + "'";
        try (ResultSet rs = stmt.executeQuery(getPluginSql)) {
            if (rs.next()) {
                String plugin = rs.getString("plugin");
                LOGGER.info("Existing slot uses plugin: " + plugin);
                return plugin;
            }
        }
        // 如果无法确定，使用默认值
        return PREFERRED_PLUGIN;
    }

    private PGReplicationStream createReplicationStream(PGConnection pgConnection) throws SQLException {
        LOGGER.info("Creating replication stream with slot: " + SLOT_NAME);

        ChainedLogicalStreamBuilder builder = pgConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(SLOT_NAME)
                .withStatusInterval(10, TimeUnit.SECONDS);

        // 根据实际使用的插件配置选项
        if ("wal2json".equals(actualPlugin)) {
            builder = builder
                    .withSlotOption("include-xids", "true")
                    .withSlotOption("include-timestamp", "true")
                    .withSlotOption("include-schemas", "true")
                    .withSlotOption("include-types", "true")
                    .withSlotOption("include-type-oids", "true")
                    .withSlotOption("write-in-chunks", "true")
                    .withSlotOption("format-version", "2");
        } else if ("test_decoding".equals(actualPlugin)) {
            builder = builder
                    .withSlotOption("include-xids", "true")
                    .withSlotOption("include-timestamp", "true")
                    .withSlotOption("skip-empty-xacts", "true");
        }

        return builder.start();
    }

    private void processWALRecords(PGReplicationStream stream) throws SQLException {
        LOGGER.info("Starting to process WAL records...");

        while (running) {
            try {
                ByteBuffer buffer = stream.readPending();

                if (buffer == null) {
                    try {
                        Thread.sleep(100L); // 短暂休眠
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.info("WAL processing interrupted");
                        return;
                    }
                    continue;
                }

                // 解析WAL记录（JSON格式）
                parseWALRecord(buffer);

                // 确认处理进度
                LogSequenceNumber lsn = stream.getLastReceiveLSN();
                stream.setAppliedLSN(lsn);
                stream.setFlushedLSN(lsn);

            } catch (SQLException e) {
                LOGGER.severe("Error processing WAL record: " + e.getMessage());
                throw e;
            }
        }

        LOGGER.info("WAL processing stopped");
    }

    /**
     * 解析WAL记录（适用于wal2json和test_decoding插件）
     */
    private void parseWALRecord(ByteBuffer buffer) {
        try {
            // 获取原始字节数组并转换为字符串
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            String walRecord = new String(data, StandardCharsets.UTF_8);

            if (walRecord.trim().isEmpty()) {
                return;
            }

            LOGGER.info("Received WAL record: " + walRecord);

            // 根据实际使用的插件类型解析
            if ("wal2json".equals(actualPlugin)) {
                parseWal2JsonRecord(walRecord);
            } else if ("test_decoding".equals(actualPlugin)) {
                parseTestDecodingRecord(walRecord);
            }

        } catch (Exception e) {
            LOGGER.severe("Error parsing WAL record: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析wal2json插件输出的JSON格式记录
     */
    private void parseWal2JsonRecord(String jsonRecord) {
        try {
            // 简单的JSON解析示例
            if (jsonRecord.contains("\"change\"")) {
                LOGGER.info("Processing change event: " + jsonRecord);

                // 这里可以使用JSON库（如Jackson或Gson）进行详细解析
                if (jsonRecord.contains("\"kind\":\"insert\"")) {
                    LOGGER.info("INSERT operation detected");
                } else if (jsonRecord.contains("\"kind\":\"update\"")) {
                    LOGGER.info("UPDATE operation detected");
                } else if (jsonRecord.contains("\"kind\":\"delete\"")) {
                    LOGGER.info("DELETE operation detected");
                }
            }
        } catch (Exception e) {
            LOGGER.warning("Failed to parse wal2json record: " + e.getMessage());
        }
    }

    /**
     * 解析test_decoding插件输出的文本格式记录
     */
    private void parseTestDecodingRecord(String textRecord) {
        try {
            LOGGER.info("Processing test_decoding record: " + textRecord);

            if (textRecord.contains("BEGIN")) {
                LOGGER.info("Transaction BEGIN");
            } else if (textRecord.contains("COMMIT")) {
                LOGGER.info("Transaction COMMIT");
            } else if (textRecord.contains("table")) {
                if (textRecord.contains("INSERT:")) {
                    LOGGER.info("INSERT operation detected");
                } else if (textRecord.contains("UPDATE:")) {
                    LOGGER.info("UPDATE operation detected");
                } else if (textRecord.contains("DELETE:")) {
                    LOGGER.info("DELETE operation detected");
                }
            }
        } catch (Exception e) {
            LOGGER.warning("Failed to parse test_decoding record: " + e.getMessage());
        }
    }

    /**
     * 清理资源
     */
    private void cleanup() {
        LOGGER.info("Cleaning up resources...");
        // 这里可以添加清理逻辑，比如删除复制槽（如果需要）
    }

}