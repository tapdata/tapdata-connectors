package io.tapdata.connector.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/** 用户反馈的完整字段、主键、分区与表参数；path 由测试本地 Catalog 分配，不访问生产 S3。 */
public final class FutureInhouseDateFixture {
    private FutureInhouseDateFixture() { }

    public static Schema schema(String expirationMode) {
        return Schema.newBuilder()
                .column("ConfirmNo", DataTypes.VARCHAR(30).notNull())
                .column("InhouseDate", DataTypes.DATE().notNull())
                .column("Rooms", DataTypes.INT())
                .column("RoomRate", DataTypes.DECIMAL(18, 2))
                .column("Resort", DataTypes.VARCHAR(16).notNull())
                .column("ExtractionDate", DataTypes.DATE().notNull())
                .column("ExtractionHour", DataTypes.INT().notNull())
                .column("LastModified", DataTypes.TIMESTAMP(6))
                .column("ods_updated_at", DataTypes.TIMESTAMP(3))
                .column("pt_extractiondate", DataTypes.INT().notNull())
                .column("op", DataTypes.VARCHAR(1024))
                .primaryKey("ConfirmNo", "InhouseDate", "Resort", "ExtractionDate",
                        "ExtractionHour", "pt_extractiondate")
                .partitionKeys("pt_extractiondate")
                .option("num-sorted-run.stop-trigger", "2147483647")
                .option("write-only", "false")
                .option("snapshot.num-retained.min", "2")
                .option("snapshot.expire.limit", "500")
                .option("write-buffer-spillable", "true")
                .option("snapshot.ignore-empty-commit", "true")
                .option("snapshot.time-retained", "12h")
                .option("compaction.offpeak.end.hour", "9")
                .option("target-file-size", "128mb")
                .option("num-sorted-run.compaction-trigger", "20")
                .option("compaction.offpeak-ratio", "10")
                .option("sink.parallelism", "1")
                .option("dynamic-bucket.initial-buckets", "1")
                .option("dynamic-bucket.target-row-num", "5000000")
                .option("changelog-producer", "none")
                .option("write-buffer-spill.max-disk-size", "5gb")
                .option("compaction.optimization-interval", "60min")
                .option("sort-spill-threshold", "10")
                .option("bucket", "-1")
                .option("changelog-producer.lookup-wait", "false")
                .option("scan.plan-sort-partition", "true")
                .option("snapshot.expire.execution-mode", expirationMode)
                .option("snapshot.num-retained.max", "30")
                .option("sort-spill-buffer-size", "64mb")
                .option("compaction.offpeak.start.hour", "2")
                .option("write-buffer-size", "128mb")
                .option("commit.force-compact", "false")
                .option("dynamic-bucket.max-buckets", "50")
                .build();
    }

    public static GenericRow row(int version) {
        int date = (int) LocalDate.of(2026, 9, 7).toEpochDay();
        Timestamp timestamp = Timestamp.fromLocalDateTime(LocalDateTime.of(2026, 9, 7, 12, 0));
        return GenericRow.of(BinaryString.fromString("C001"), date, version,
                Decimal.fromBigDecimal(new BigDecimal("123.45"), 18, 2),
                BinaryString.fromString("R01"), date, 12, timestamp, timestamp,
                20260907, BinaryString.fromString("u"));
    }
}
