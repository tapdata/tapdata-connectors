package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.cdc.process.analyse.NormalFileReader;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

public class DMLManager implements Activity {
    public static final String DML_DATA_FILE_NAME_MATCH = "(CDC)([\\d]*)(\\.json)";
    final ProcessHandler handler;
    final int threadCount;
    private final String database;
    private final List<String> table;
    String basePath;
    final AtomicReference<Throwable> throwableCollector;
    final BooleanSupplier alive;
    final NormalFileReader reader;
    ScheduledFuture<?> scheduledFuture;
    Log log;

    protected DMLManager(ProcessHandler handler,
                         String basePath,
                         int threadCount) {
        if (threadCount > 10 || threadCount < 1) threadCount = 3;
        this.handler = handler;
        this.throwableCollector = handler.processInfo.throwableCollector;
        this.threadCount = threadCount;
        this.table = null == handler.processInfo.cdcTable ? new ArrayList<>() : handler.processInfo.cdcTable;
        this.database = handler.processInfo.database;
        this.alive = handler.processInfo.alive;
        this.reader = new NormalFileReader();
        this.log = handler.processInfo.nodeContext.getLog();
        withBasePath(basePath);
    }
    protected void withBasePath(String basePath) {
        if (null == basePath) {
            basePath = "";
        }
        if (basePath.endsWith(File.separator)) {
            basePath = basePath.substring(0, basePath.length() - File.separator.length());
        }
        this.basePath = basePath;
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public void doActivity() {
        cancelSchedule(scheduledFuture, log);
        scheduledFuture = this.handler.getScheduledExecutorService().scheduleWithFixedDelay(() -> {
            try {
                readOnce();
            } catch (Throwable t) {
                synchronized (throwableCollector) {
                    throwableCollector.set(t);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    protected String getFullBasePath() {
        return FileUtil.paths(basePath, database);
    }

    protected void readOnce() {
        //get all file
        File path = new File(getFullBasePath());
        if (!path.isDirectory()) {
            return;
        }
        LinkedBlockingQueue<File> jsonFilePath = new LinkedBlockingQueue<>(scanAllTableDir(path));
        if (!alive.getAsBoolean() || jsonFilePath.isEmpty()) {
            return;
        }
        CompletableFuture<Void>[] all = new CompletableFuture[this.threadCount];
        for (int index = 0; index < this.threadCount; index++) {
            all[index] = CompletableFuture.runAsync(() -> doOnce(jsonFilePath));
        }
        CompletableFuture<Void> completableFuture = CompletableFuture.allOf(all);
        try {
            completableFuture.get();
        } catch (InterruptedException | ExecutionException interruptedException) {
            throwableCollector.set(interruptedException);
        }
    }

    public void doOnce(LinkedBlockingQueue<File> jsonFilePath) {
        while (alive.getAsBoolean() && !jsonFilePath.isEmpty()) {
            File tableDir = jsonFilePath.poll();
            if (null == tableDir) continue;
            String tableNameFromDir = tableDir.getName();
            String tableVersion = handler.judgeTableVersion(tableNameFromDir);
            if (null == tableVersion) {
                continue;
            }
            String tableVersionDMLDataPath = FileUtil.paths(tableDir.getAbsolutePath(), tableVersion);
            File tableVersionDMLDataDir = new File(tableVersionDMLDataPath);
            if (tableVersionDMLDataDir.exists() && tableVersionDMLDataDir.isDirectory()) {
                File[] jsonFiles = tableVersionDMLDataDir.listFiles(f -> f.exists()
                        && f.isFile()
                        && f.getName().matches(DML_DATA_FILE_NAME_MATCH));
                if (null != jsonFiles && jsonFiles.length > 0) {
                    new ArrayList<>(Arrays.asList(jsonFiles))
                            .stream()
                            .sorted((j1, j2) -> j1.getName().compareToIgnoreCase(j2.getName()))
                            .forEach(this::readJsonAndCommit);
                }
            }
        }
    }

    protected void readJsonAndCommit(File file) {
        this.reader.readLineByLine(file, line -> {
            DMLObject dmlObject = TapSimplify.fromJson(line, DMLObject.class);
            String table = dmlObject.getTable();
            DDLManager.VersionInfo versionInfo = handler.tableVersionMap.get(table);
            if (null != versionInfo) {
                dmlObject.setTableColumnInfo(versionInfo.info);
            } else {
                log.debug("Can not find table version info from ddl object, dml info: {}", line);
            }
            handler.getTapEventManager().emit(dmlObject);
        }, alive);
        try {
            FileUtils.delete(file);
        } catch (IOException e) {
            log.error("A read complete file delete failed in cdc, file: {}, message: {}", file.getPath(), e.getMessage());
        }
    }

    /**
     * base path: ${tap-dir}/${ProcessHandler.BASE_CDC_DATA_DIR}
     */
    public List<File> scanAllTableDir(File databaseDir) {
        File[] files = databaseDir.listFiles(f -> f.exists() && f.isDirectory());
        return null != files ?
                new ArrayList<>(Arrays.asList(files))
                        .stream()
                        .filter(f -> table.isEmpty() || table.contains(f.getName()))
                        .collect(Collectors.toList())
                : new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        cancelSchedule(scheduledFuture, log);
    }
}
