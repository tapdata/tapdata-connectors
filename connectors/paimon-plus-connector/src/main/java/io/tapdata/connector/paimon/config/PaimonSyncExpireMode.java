package io.tapdata.connector.paimon.config;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** 连接器只支持同步快照维护；校验不得修改已有表的 metadata。 */
public final class PaimonSyncExpireMode {
    public static final String KEY = "snapshot.expire.execution-mode";

    private PaimonSyncExpireMode() {}

    public static void requireSync(String tableKey, Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException("Only FileStoreTable supports connector writes: " + tableKey);
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        validate(tableKey, fileStoreTable.options().get(KEY),
                () -> fileStoreTable.coreOptions().snapshotExpireExecutionMode());
    }

    public static void requireSync(String tableKey, Map<String, String> options) {
        validate(tableKey, options.get(KEY),
                () -> CoreOptions.fromMap(options).snapshotExpireExecutionMode());
    }

    public static void requireSyncProperties(String tableKey, List<? extends Map<String, String>> properties) {
        Map<String, String> options = new java.util.HashMap<>();
        if (properties != null) {
            for (Map<String, String> property : properties) {
                if (KEY.equals(property.get("propKey"))) {
                    // 与 schemaBuilder.option 相同的后值覆盖语义；显式空值也必须报错。
                    String value = property.get("propValue");
                    if (value == null || value.isEmpty()) {
                        throw unsupported(tableKey, value, null);
                    }
                    options.put(KEY, value);
                }
            }
        }
        requireSync(tableKey, options);
    }

    private static void validate(String tableKey, String value,
            Supplier<CoreOptions.ExpireExecutionMode> mode) {
        // Paimon 1.3.2 CoreOptions 使用原生枚举解析，缺省为 SYNC；TableCommitImpl 在 SYNC
        // 分支用直接执行器运行 maintenance。只接受这个有效模式，不捕获内部 maintenance executor。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L435
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L118
        CoreOptions.ExpireExecutionMode effective;
        try {
            effective = mode.get();
        } catch (IllegalArgumentException invalid) {
            throw unsupported(tableKey, value, invalid);
        }
        if (effective != CoreOptions.ExpireExecutionMode.SYNC) {
            throw unsupported(tableKey, value == null ? String.valueOf(effective) : value, null);
        }
    }

    private static IllegalArgumentException unsupported(String tableKey, String value, Throwable cause) {
        return new IllegalArgumentException("Paimon table " + tableKey + ": " + KEY + "=" + value
                + "，此连接器仅支持 SYNC", cause);
    }
}
