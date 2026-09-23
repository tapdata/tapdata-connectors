package io.tapdata.connector.paimon.config;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** 仅使用 Paimon 原生规则校验维护模式，不选择模式或修改表配置。 */
public final class PaimonExpireMode {
    public static final String KEY = "snapshot.expire.execution-mode";

    private PaimonExpireMode() {}

    public static void validate(String tableKey, Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException("Only FileStoreTable supports connector writes: " + tableKey);
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        validate(tableKey, fileStoreTable.options().get(KEY),
                () -> fileStoreTable.coreOptions().snapshotExpireExecutionMode());
    }

    public static void validate(String tableKey, Map<String, String> options) {
        validate(tableKey, options.get(KEY),
                () -> CoreOptions.fromMap(options).snapshotExpireExecutionMode());
    }

    public static void validateProperties(String tableKey, List<? extends Map<String, String>> properties) {
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
        validate(tableKey, options);
    }

    private static void validate(String tableKey, String value,
            Supplier<CoreOptions.ExpireExecutionMode> mode) {
        // 原生支持 SYNC 和 ASYNC；这里只触发原生解析，不添加连接器模式限制。
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java
        try { mode.get(); }
        catch (IllegalArgumentException invalid) { throw unsupported(tableKey, value, invalid); }
    }

    private static IllegalArgumentException unsupported(String tableKey, String value, Throwable cause) {
        return new IllegalArgumentException("Paimon table " + tableKey + ": " + KEY + "=" + value
                + "，Paimon 原生合法值为 SYNC 或 ASYNC", cause);
    }
}
