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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class DDLManager implements Activity {
    public static final String TABLE_VERSION_FILE_NAME_MATCH = "(schema_)([\\d]{18})(_)([\\d]{10})(\\.json)";
    private final List<String> cdcTable;
    private final String database;
    private final String basePath;
    private final ProcessHandler handler;
    private ScheduledFuture<?> scheduledFuture;
    final NormalFileReader reader;
    final Log log;
    final AtomicReference<Throwable> throwableCollector;

    protected DDLManager(ProcessHandler handler, String basePath) {
        this.handler = handler;
        this.cdcTable = handler.processInfo.cdcTable;
        this.database = handler.processInfo.database;
        this.basePath = basePath;
        this.throwableCollector = handler.processInfo.throwableCollector;
        this.log = handler.processInfo.nodeContext.getLog();
        this.reader = new NormalFileReader();
    }

    @Override
    public void init() {
        // nothing need to do
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
        } catch (Exception t) {
            synchronized (throwableCollector) {
                throwableCollector.set(t);
            }
        }
    }

    protected List<DDLObject> getSchemaJsonFile(File metaDir) {
        File[] schemaJson = metaDir.listFiles(f -> {
            String name = f.getName();
            return f.isFile() && name.matches(TABLE_VERSION_FILE_NAME_MATCH);
        });
        List<DDLObject> schemaJsonFile = new ArrayList<>();
        if (null == schemaJson || schemaJson.length <= 0) return schemaJsonFile;
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
        return schemaJsonFile.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    //"${basePath}/${database}/${table}/meta"
    protected void scanTableDDL(File tableDir) {
        if (!tableDir.exists() || !tableDir.isDirectory()) return;
        String tableName = tableDir.getName();
        File metaDir = new File(FileUtil.paths(tableDir.getAbsolutePath(), "meta"));
        List<DDLObject> schemaJsonFile = getSchemaJsonFile(metaDir);
        if (schemaJsonFile.isEmpty()) return;
        schemaJsonFile.sort(Comparator.comparing(DDLObject::getTableVersion));
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
                            newDDLTableVersion.getTableColumns(),
                            handler.processInfo.timezone
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

        public VersionInfo(String v, List<Map<String, Object>> info, TimeZone timezone) {
            this.version = v;
            this.info = new HashMap<>();
            if (CollectionUtils.isNotEmpty(info)) {
                for (Map<String, Object> convertInfo : info) {
                    String columnName = String.valueOf(convertInfo.get(Convert.COLUMN_NAME));
                    this.info.put(columnName, Convert.instance(convertInfo, timezone));
                }
            }
        }
    }
}
