package io.tapdata.connector.paimon.config;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;

/** 连接器写缓冲开关的唯一映射，不修改 Catalog 或传入的 Table。 */
public final class PaimonWriteOptions {
    private PaimonWriteOptions() {}

    public static String spillValue(PaimonConfig config) {
        // 原生默认 true，连接器缺省/null 表示关闭，因此必须显式输出 false。
        // https://paimon.apache.org/docs/1.3/maintenance/configurations/#write-buffer-spillable
        return Boolean.toString(Boolean.TRUE.equals(config.getDiskOverflowWrite()));
    }

    public static FileStoreTable runtimeWriteTable(FileStoreTable original, PaimonConfig config) {
        // Paimon 1.3.2 copyWithoutTimeTravel 校验可变选项并复制当前 TableSchema，不调用 Catalog ALTER。
        // 只覆盖 spillable；不让读取的时间旅行参数将写入副本切到历史 schema；保留限额和维护模式。
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java
        return original.copyWithoutTimeTravel(Collections.singletonMap(
                CoreOptions.WRITE_BUFFER_SPILLABLE.key(), spillValue(config)));
    }
}
