package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.common.util.FileUtil;
import io.tapdata.entity.logger.Log;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

public interface Activity extends AutoCloseable {
    void init();
    void doActivity();

    default void cancelSchedule(ScheduledFuture<?> future, Log log) {
        if (Objects.nonNull(future)) {
            try {
                future.cancel(true);
            } catch (Exception e1){
                log.warn("Scheduled cancel failed: {}", e1.getMessage());
            }
        }
    }

    default List<File> scanAllCdcTableDir(List<String> cdcTable, File databaseDir, Supplier<Boolean> alive) {
        List<File> tableDirs = new ArrayList<>();
        if (org.apache.commons.collections.CollectionUtils.isEmpty(cdcTable)) {
            File[] tableFiles = databaseDir.listFiles(File::isDirectory);
            if (null != tableFiles && tableFiles.length > 0) {
                tableDirs.addAll(new ArrayList<>(Arrays.asList(tableFiles)));
            }
        } else {
            for (String tableName : cdcTable) {
                if (!alive.get()) {
                    break;
                }
                File file = new File(FileUtil.paths(databaseDir.getAbsolutePath(), tableName));
                if (file.exists() && file.isDirectory()) {
                    tableDirs.add(file);
                }
            }
        }
        return tableDirs;
    }
}
