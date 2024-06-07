//package io.tapdata.connector.tidb.cdc.process.thread;
//
//import io.tapdata.common.util.FileUtil;
//import io.tapdata.connector.tidb.cdc.process.TiData;
//import io.tapdata.connector.tidb.cdc.process.analyse.NormalFileReader;
//import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
//import io.tapdata.connector.tidb.cdc.process.split.SplitByFileSizeImpl;
//import io.tapdata.entity.logger.Log;
//import io.tapdata.entity.simplify.TapSimplify;
//import org.apache.commons.io.FileUtils;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.function.Supplier;
//
//class DMLManagerIgnore implements Activity {
//    public static final String DML_DATA_FILE_NAME_MATCH = "(CDC)([\\d]*)(\\.json)";
//    final ProcessHandler handler;
//    final int threadCount;
//    ConcurrentHashMap<String, LinkedBlockingQueue<DMLObject>> data;
//    private final String database;
//    private final List<String> table;
//    String basePath;
//    final AtomicReference<Throwable> throwableCollector;
//    final Supplier<Boolean> alive;
//    final NormalFileReader reader;
//    ScheduledFuture<?> scheduledFuture;
//    Log log;
//    final int eachPriceSize;
//
//    protected DMLManagerIgnore(ProcessHandler handler,
//                               String basePath,
//                               int eachPriceSize,
//                               int threadCount) {
//        if (threadCount > 10 || threadCount < 1) threadCount = 3;
//        if (eachPriceSize > 5000 || eachPriceSize < 1) eachPriceSize = 1000;
//        this.eachPriceSize = eachPriceSize;
//        this.handler = handler;
//        this.throwableCollector = handler.processInfo.throwableCollector;
//        this.threadCount = threadCount;
//        this.data = new ConcurrentHashMap<>();
//        this.table = handler.processInfo.cdcTable;
//        this.database = handler.processInfo.database;
//        this.alive = handler.processInfo.alive;
//        this.reader = new NormalFileReader();
//        this.log = handler.processInfo.nodeContext.getLog();
//        this.reader.setLog(log);
//        withBasePath(basePath);
//    }
//
//    public ConcurrentHashMap<String, LinkedBlockingQueue<DMLObject>> getData() {
//        return data;
//    }
//
//    protected void withBasePath(String basePath) {
//        if (null == basePath) {
//            basePath = "";
//        }
//        if (basePath.endsWith(File.separator)) {
//            basePath = basePath.substring(0, basePath.length() - File.separator.length());
//        }
//        this.basePath = basePath;
//    }
//
//    @Override
//    public void init() {
//
//    }
//
//    @Override
//    public void doActivity() {
//        cancelSchedule(scheduledFuture, log);
//        scheduledFuture = this.handler.getScheduledExecutorService().scheduleWithFixedDelay(() -> {
//            try {
//                readOnce();
//            } catch (Throwable t) {
//                synchronized (throwableCollector) {
//                    throwableCollector.set(t);
//                }
//            }
//        }, 1, 1, TimeUnit.SECONDS);
//    }
//
//    public List<List<File>> splitToPieces(List<File> jsonFilePath) {
//        SplitByFileSizeImpl splitByFileSize = new SplitByFileSizeImpl(threadCount);
//        return splitByFileSize.splitToPieces(jsonFilePath, 0);
//    }
//
//    /**
//     * base path: ${tap-dir}/${ProcessHandler.BASE_CDC_DATA_DIR}
//     */
//    public List<File> scanAllJson(File databaseDir) {
//        List<File> files = new ArrayList<>();
//        if (!databaseDir.exists() || !databaseDir.isDirectory()) {
//            return files;
//        }
//        List<File> tableDirs = scanAllCdcTableDir(table, databaseDir, handler.processInfo.alive);
//        if (tableDirs.isEmpty()) {
//            return files;
//        }
//
//        for (File tableDir : tableDirs) {
//            if (!alive.get()) {
//                break;
//            }
//            String tableNameFromDir = tableDir.getName();
//            String tableVersion = handler.judgeTableVersion(tableNameFromDir);
//            if (null == tableVersion) {
//                continue;
//            }
//            String tableVersionDMLDataPath = FileUtil.paths(tableDir.getAbsolutePath(), tableVersion);
//            File tableVersionDMLDataDir = new File(tableVersionDMLDataPath);
//            if (tableVersionDMLDataDir.exists() && tableVersionDMLDataDir.isDirectory()) {
//                File[] jsonFiles = tableVersionDMLDataDir.listFiles(f -> f.exists()
//                        && f.isFile()
//                        && f.getName().matches(DML_DATA_FILE_NAME_MATCH));
//                if (null != jsonFiles && jsonFiles.length > 0) {
//                    files.addAll(new ArrayList<>(Arrays.asList(jsonFiles)));
//                }
//            }
//        }
//        return files;
//    }
//
//    protected void readOnce() {
//        //get all file
//        File path = new File(getFullBasePath());
//        if (!path.isDirectory()) {
//            return;
//        }
//        List<File> jsonFilePath = scanAllJson(path);
//        if (!alive.get() || jsonFilePath.isEmpty()) {
//            return;
//        }
//        List<List<File>> partition = splitToPieces(jsonFilePath);
//        // each thread do read once()
//        CompletableFuture<Void>[] all = new CompletableFuture[partition.size()];
//        for (int index = 0; index < partition.size(); index++) {
//            int finalIndex = index;
//            all[index] = CompletableFuture.runAsync(() -> readPartition(partition.get(finalIndex)));
//        }
//        CompletableFuture<Void> completableFuture = CompletableFuture.allOf(all);
//
//
//        Map<String, List<? extends TiData>> map = new HashMap<>();
//        completableFuture.thenRun(() -> data.forEach((key, list) -> map.put(key, sort(new ArrayList<>(list)))));
//        try {
//            completableFuture.get();
//        } catch (InterruptedException | ExecutionException interruptedException) {
//            throwableCollector.set(interruptedException);
//        } finally {
//            handler.getTapEventManager().emit(map);
//            data.clear();
//        }
//    }
//
//    public List<DMLObject> sort(List<DMLObject> arr) {
//        if (null == arr) return new ArrayList<>();
//        if (arr.isEmpty()) return arr;
//        arr.sort((d1, d2) -> {
//            if (null == d1 || null == d1.getTs()) return -1;
//            if (null == d2 || null == d2.getTs()) return 1;
//            return d1.getTs().compareTo(d2.getTs());
//        });
//        return arr;
//    }
//
//    protected void readPartition(List<File> jsonFileList) {
//        if (!alive.get()) {
//            return;
//        }
//        for (File file : jsonFileList) {
//            this.reader.readLineByLine(file, line -> {
//                DMLObject dmlObject = TapSimplify.fromJson(line, DMLObject.class);
//                String table = dmlObject.getTable();
//                DDLManager.VersionInfo versionInfo = handler.tableVersionMap.get(table);
//                if (null != versionInfo) {
//                    dmlObject.setTableColumnInfo(versionInfo.info);
//                } else {
//                    log.debug("Can not find table version info from ddl object, dml info: {}", line);
//                }
//                LinkedBlockingQueue<DMLObject> linkedQueue = data.computeIfAbsent(table, key -> new LinkedBlockingQueue<>(this.eachPriceSize));
//                linkedQueue.add(dmlObject);
//            }, alive);
//            try {
//                FileUtils.delete(file);
//            } catch (IOException e) {
//                log.error("A read complete file delete failed in cdc, file: {}, message: {}", file.getPath(), e.getMessage());
//            }
//        }
//    }
//
//    protected String getFullBasePath() {
//        return FileUtil.paths(basePath, database);
//    }
//
//    @Override
//    public void close() throws Exception {
//        cancelSchedule(scheduledFuture, log);
//    }
//}
