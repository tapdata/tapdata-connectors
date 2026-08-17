package io.tapdata.connector.paimon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用 Spark 3.5 手动查询 MinIO 上的 Paimon 表。
 *
 * <p>运行前请修改下方静态变量，并移除 {@link Disabled}。清空表方法还要求显式开启
 * {@code ALLOW_CLEAR_TABLE}，该测试连接真实环境，默认不参与 CI。
 * 使用 JDK 17 时，需在测试 VM options 中加入
 * {@code --add-opens=java.base/sun.nio.ch=ALL-UNNAMED}。
 */
@Disabled("手动连接真实 MinIO/Paimon 表；修改类顶部配置后移除此注解")
class PaimonSpark35MinioQueryTest {

    // ===== Paimon Catalog / MinIO 配置：运行前直接修改，切勿提交真实凭证 =====
    private static final String CATALOG_NAME = "paimon";
    private static final String WAREHOUSE = "s3://paimon-test/warehouse/";
    private static final String S3_ENDPOINT = "http://127.0.0.1:9000";
    private static final String S3_ACCESS_KEY = "tapdata-minio-test";
    private static final String S3_SECRET_KEY = "7041b93b9ae2cb383db69f45fc05185503be4de37bb3b745";
    private static final String S3_REGION = "";
    private static final boolean S3_PATH_STYLE_ACCESS = true;

    // ===== 查询配置：可按核对目标随时修改 =====
    private static final String DATABASE_NAME = "default";
    private static final String TABLE_NAME = "cdc_paimon_all_types";
    private static final String TABLE_IDENTIFIER =
            CATALOG_NAME + "." + DATABASE_NAME + "." + TABLE_NAME;
    private static final String QUERY_SQL = "SELECT * FROM " + TABLE_IDENTIFIER + " LIMIT 100";
    private static final int MAX_OUTPUT_ROWS = 100;
    private static final String SESSION_TIME_ZONE = "Asia/Shanghai";

    // ===== 清空配置：默认关闭，确认目标表后才允许开启 =====
    private static final boolean ALLOW_CLEAR_TABLE = false;
    private static final String CLEAR_TABLE_SQL = "TRUNCATE TABLE " + TABLE_IDENTIFIER;

    @Test
    void queryPaimonTableForManualVerification() {
        validateConfiguration();

        SparkSession spark = createSparkSession();
        try {
            Dataset<Row> result = spark.sql(QUERY_SQL);

            System.out.println("Spark version: " + spark.version());
            System.out.println("Query SQL: " + QUERY_SQL);
            System.out.println("Result schema:");
            result.printSchema();
            System.out.println("Query result (up to " + MAX_OUTPUT_ROWS + " rows):");
            result.show(MAX_OUTPUT_ROWS, false);
        } finally {
            stopSparkSession(spark);
        }
    }

    @Test
    void clearPaimonTableForManualVerification() {
        if (!ALLOW_CLEAR_TABLE) {
            throw new IllegalStateException(
                    "清空表操作默认关闭；确认目标表后请将 ALLOW_CLEAR_TABLE 改为 true，再移除类级 @Disabled");
        }
        validateConfiguration();

        SparkSession spark = createSparkSession();
        try {
            spark.sql(CLEAR_TABLE_SQL);
            long remainingRows =
                    spark.sql("SELECT COUNT(*) AS remaining_rows FROM " + TABLE_IDENTIFIER)
                            .collectAsList()
                            .get(0)
                            .getLong(0);
            if (remainingRows != 0L) {
                throw new IllegalStateException(
                        "清空表后仍有 " + remainingRows + " 行数据，表标识: " + TABLE_IDENTIFIER);
            }
            System.out.println("Paimon 表已清空（表结构保留）: " + TABLE_IDENTIFIER);
        } finally {
            stopSparkSession(spark);
        }
    }

    private static SparkSession createSparkSession() {
        String catalogPrefix = "spark.sql.catalog." + CATALOG_NAME;
        SparkSession.Builder builder =
                SparkSession.builder()
                        .appName("paimon-spark-3.5-minio-manual-query")
                        .master("local[2]")
                        .config("spark.ui.enabled", "false")
                        .config("spark.driver.bindAddress", "127.0.0.1")
                        .config("spark.sql.shuffle.partitions", "4")
                        .config("spark.sql.session.timeZone", SESSION_TIME_ZONE)
                        .config(
                                "spark.redaction.regex",
                                "(?i)secret|password|token|access[._-]?key")
                        .config(
                                "spark.sql.extensions",
                                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                        .config(catalogPrefix, "org.apache.paimon.spark.SparkCatalog")
                        .config(catalogPrefix + ".warehouse", WAREHOUSE)
                        .config(catalogPrefix + ".s3.endpoint", S3_ENDPOINT)
                        .config(catalogPrefix + ".s3.access-key", S3_ACCESS_KEY)
                        .config(catalogPrefix + ".s3.secret-key", S3_SECRET_KEY)
                        .config(
                                catalogPrefix + ".s3.path.style.access",
                                Boolean.toString(S3_PATH_STYLE_ACCESS));

        if (!S3_REGION.trim().isEmpty()) {
            builder.config(catalogPrefix + ".s3.region", S3_REGION);
        }
        return builder.getOrCreate();
    }

    private static void stopSparkSession(SparkSession spark) {
        spark.stop();
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    private static void validateConfiguration() {
        List<String> unconfiguredFields = new ArrayList<>();
        requireConfigured("CATALOG_NAME", CATALOG_NAME, unconfiguredFields);
        requireConfigured("WAREHOUSE", WAREHOUSE, unconfiguredFields);
        requireConfigured("S3_ENDPOINT", S3_ENDPOINT, unconfiguredFields);
        requireConfigured("S3_ACCESS_KEY", S3_ACCESS_KEY, unconfiguredFields);
        requireConfigured("S3_SECRET_KEY", S3_SECRET_KEY, unconfiguredFields);
        requireConfigured("DATABASE_NAME", DATABASE_NAME, unconfiguredFields);
        requireConfigured("TABLE_NAME", TABLE_NAME, unconfiguredFields);
        requireConfigured("QUERY_SQL", QUERY_SQL, unconfiguredFields);

        if (!unconfiguredFields.isEmpty()) {
            throw new IllegalStateException(
                    "请先修改测试类顶部的配置项: " + String.join(", ", unconfiguredFields));
        }
        if (!WAREHOUSE.startsWith("s3://")) {
            throw new IllegalStateException("WAREHOUSE 必须使用 Paimon 原生 s3:// 协议");
        }
    }

    private static void requireConfigured(
            String fieldName, String value, List<String> unconfiguredFields) {
        if (value == null
                || value.trim().isEmpty()
                || (value.contains("<") && value.contains(">"))) {
            unconfiguredFields.add(fieldName);
        }
    }
}
