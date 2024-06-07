package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.cdc.process.analyse.NormalFileReader;
import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import org.apache.commons.collections4.CollectionUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class DDLManager implements Activity {
    public static final String TABLE_VERSION_FILE_NAME_MATCH = "(schema_)([\\d]{18})(_)([\\d]{10})(\\.json)";
    private final List<String> cdcTable;
    private final String database;
    private final String basePath;
    private final ProcessHandler handler;
    private ScheduledFuture<?> scheduledFuture;
    final NormalFileReader reader;
    private final ConcurrentHashMap<String, String> currentTableVersion;
    final Log log;
    final AtomicReference<Throwable> throwableCollector;

    protected DDLManager(ProcessHandler handler, String basePath) {
        this.handler = handler;
        this.cdcTable = handler.processInfo.cdcTable;
        this.database = handler.processInfo.database;
        this.basePath = basePath;
        this.throwableCollector = handler.processInfo.throwableCollector;
        this.log = handler.processInfo.nodeContext.getLog();
        this.currentTableVersion = new ConcurrentHashMap<>();
        this.reader = new NormalFileReader();
        this.reader.setLog(log);
    }

    @Override
    public void init() {

    }

    @Override
    public void doActivity() {
        cancelSchedule(this.scheduledFuture, log);
        this.scheduledFuture = this.handler.getScheduledExecutorService().scheduleWithFixedDelay(
                this::scanDDL,
                0,
                1,
                TimeUnit.SECONDS
        );
    }

    protected void scanDDL() {
        try {
            List<File> tableDirs = getTableDirs();
            if (tableDirs.isEmpty()) return;
            for (File tableDir : tableDirs) {
                scanTableDDL(tableDir);
            }
        } catch (Throwable t) {
            synchronized (throwableCollector) {
                throwableCollector.set(t);
            }
        }
    }

    //"${basePath}/${database}/${table}/meta"
    protected void scanTableDDL(File tableDir) {
        if (!tableDir.exists() || !tableDir.isDirectory()) return;
        String tableName = tableDir.getName();
        File metaDir = new File(FileUtil.paths(tableDir.getAbsolutePath(), "meta"));
        File[] schemaJson = metaDir.listFiles(f -> {
            String name = f.getName();
            return f.isFile() && name.matches(TABLE_VERSION_FILE_NAME_MATCH);
        });
        if (null == schemaJson || schemaJson.length <= 0) return;
        List<DDLObject> schemaJsonFile = new ArrayList<>();
        for (File file : schemaJson) {
            String tableDDL = this.reader.readAll(file, handler.processInfo.alive);
            try {
                DDLObject ddlObject = TapSimplify.fromJson(tableDDL, DDLObject.class);
                if (null == ddlObject.getTableVersion()) {
                    log.warn("A cdc ddl data not contains TableVersion, ddl: {}", tableDDL);
                    continue;
                }
                schemaJsonFile.add(ddlObject);
            } catch (Exception e) {
                log.warn("A cdc ddl data can not be parse as a Json Object, ddl: {}", tableDDL);
            }
        }
        if (schemaJsonFile.isEmpty()) return;
        schemaJsonFile.sort((s1, s2) -> {
            if (null == s1 || null == s1.getTableVersion()) return -1;
            if (null == s2 || null == s2.getTableVersion()) return 1;
            return s1.getTableVersion().compareTo(s2.getTableVersion());
        });
        DDLObject newDDLTableVersion;
        String oldDDLTableVersion = handler.judgeTableVersion(tableName);
        if (null == oldDDLTableVersion) {
            newDDLTableVersion = schemaJsonFile.get(0);
        } else {
            List<DDLObject> ddlObjects = new ArrayList<>(schemaJsonFile);
            for (DDLObject ddlObject : schemaJsonFile) {
                Long tableVersion = ddlObject.getTableVersion();
                //排除旧版本
                ddlObjects.remove(ddlObject);
                if (tableVersion.compareTo(Long.parseLong(oldDDLTableVersion)) >= 0) {
                    break;
                }
            }
            if (ddlObjects.isEmpty()) {
                return;
            }
            //找到一个新版本
            newDDLTableVersion = ddlObjects.get(0);
            //旧版本还有DML数据则退出
            if (handler.judgeTableVersionHasDMlData(tableDir, oldDDLTableVersion)) {
                return;
            }
        }
        //旧版本为null
        //或者
        //旧版本没有DML数据则更新到新版本模型，DML开始监听新模型的cdc数据
        handler.getTapEventManager().emit(newDDLTableVersion);
        synchronized (handler.tableVersionLock) {
            handler.tableVersionMap.put(
                    tableName,
                    new VersionInfo(
                            String.valueOf(newDDLTableVersion.getTableVersion()),
                            newDDLTableVersion.getTableColumns()
                    )
            );
        }
    }

    protected List<File> getTableDirs() {
        String databasePath = FileUtil.paths(basePath, database);
        File databaseDir = new File(databasePath);
        return scanAllCdcTableDir(cdcTable, databaseDir, handler.processInfo.alive);
    }

    @Override
    public void close() throws Exception {
        cancelSchedule(this.scheduledFuture, log);
        this.scheduledFuture = null;
    }

    public static class VersionInfo {
        String version;
        Map<String, Convert> info;

        public VersionInfo(String v, List<Map<String, Object>> info) {
            this.version = v;
            this.info = new HashMap<>();
            if (CollectionUtils.isNotEmpty(info)) {
                for (Map<String, Object> convertInfo : info) {
                    String columnName = String.valueOf(convertInfo.get(Convert.COLUMN_NAME));
                    this.info.put(columnName, Convert.instance(convertInfo));
                }
            }
        }
    }
}
