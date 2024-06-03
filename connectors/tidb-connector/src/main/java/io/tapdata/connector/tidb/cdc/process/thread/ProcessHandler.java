package io.tapdata.connector.tidb.cdc.process.thread;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.connector.tidb.util.pojo.ReplicaConfig;
import io.tapdata.connector.tidb.util.pojo.Sink;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class ProcessHandler implements Activity {
    private final ScheduledExecutorService scheduledExecutorService;
    protected static final String BASE_CDC_DATA_DIR = "run-resources/ti-db/cdc/%s"; //"/Users/xiao/Documents/GitHub/kit/tidb/%s"
    public final Object tableVersionLock = new Object();
    final ProcessInfo processInfo;
    final ConcurrentHashMap<String, DDLManager.VersionInfo> tableVersionMap;

    final DDLManager ddlManager;
    final DMLManager dmlManager;
    final TapEventManager tapEventManager;
    final Log log;
    final String basePath;

    public ProcessHandler(ProcessInfo processInfo, StreamReadConsumer consumer) {
        this.processInfo = processInfo;
        this.tableVersionMap = new ConcurrentHashMap<>();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.tapEventManager = new TapEventManager(this, 500, consumer);
        this.basePath = String.format(BASE_CDC_DATA_DIR, processInfo.feedId);
        this.ddlManager = new DDLManager(this, basePath);
        this.dmlManager = new DMLManager(this, basePath, 3);
        this.log = processInfo.nodeContext.getLog();
    }

    protected String judgeTableVersion(String tableName) {
        String tableVersion = null;
        synchronized (tableVersionLock) {
            if (tableVersionMap.containsKey(tableName)
                    && null != tableVersionMap.get(tableName)
                    && null != (tableVersion = tableVersionMap.get(tableName).version)) {
                return tableVersion;
            }
            return null;
        }
    }

    protected boolean judgeTableVersionHasDMlData(File tableDir, String version) {
        File file = new File(FileUtil.paths(tableDir.getAbsolutePath(), version));
        if (!file.exists() || !file.isDirectory()) return false;
        File[] files = file.listFiles(f -> f.exists() && f.isFile() && f.getName().matches(DMLManager.DML_DATA_FILE_NAME_MATCH));
        return null != files && files.length > 0;
    }

    @Override
    public void close() throws Exception {
        closeOnce(this.ddlManager);
        closeOnce(this.dmlManager);
        closeOnce(this.tapEventManager);
        try {
            scheduledExecutorService.shutdownNow();
        } catch (Exception e) {
            log.debug("Failed to shutdown ScheduledExecutorService in ProcessHandler, message: {}", e.getMessage(), e);
        }
    }

    protected void closeOnce(Activity activity) {
        stopFeedProcess();
        Optional.ofNullable(activity).ifPresent(a -> {
            try {
                a.close();
            } catch (Exception e) {
                log.debug("Failed to close ScheduledExecutorService sub-thread in ProcessHandler, message: {}", e.getMessage(), e);
            }
        });
    }

    @Override
    public void init() {
        try {
            stopFeedProcess();
        } catch (Exception e) {
            throw new CoreException("The remaining resources cannot be released, message: {}", e.getMessage(), e);
        }
        this.ddlManager.init();
        this.dmlManager.init();
        this.tapEventManager.init();
    }

    @Override
    public void doActivity() {
        startFeedProcess();
        this.ddlManager.doActivity();
        this.dmlManager.doActivity();
        this.tapEventManager.doActivity();
    }

    protected void startFeedProcess() {
        try(HttpUtil httpUtil = new HttpUtil(log)) {
            ChangeFeed changefeed = new ChangeFeed();
            changefeed.setSinkUri(String.format("file://%s?protocol=canal-json", new File(basePath).getAbsolutePath()));
            changefeed.setChangefeedId(processInfo.feedId);
            changefeed.setForceReplicate(true);
            changefeed.setSyncDdl(true);
            JSONObject jsonObject = new JSONObject();
            List<String> cdcTable = processInfo.cdcTable;
            String database = processInfo.database;
            List<String> rules = new ArrayList<>();
            if (CollectionUtils.isEmpty(cdcTable)) {
                rules.add(String.format("%s.*", database));
            } else {
                for (String table : cdcTable) {
                    String rule = String.format("%s.%s", database, table);
                    rules.add(rule);
                }
            }
            jsonObject.put("rules", rules.toArray());
            ReplicaConfig replicaConfig = new ReplicaConfig();
            replicaConfig.setFilter(jsonObject);
            Sink sink = new Sink();
            sink.setDateSeparator("none");
            sink.setProtocol("canal-json");
            replicaConfig.setSink(sink);
            changefeed.setReplicaConfig(replicaConfig);
            Long minOffset = TapEventManager.TiOffset.getMinOffset(processInfo.cdcOffset);
            if (null != minOffset) {
                changefeed.setStartTs(minOffset);
            }
            if (httpUtil.createChangefeed(changefeed, processInfo.cdcServer)) {
                log.info("Cdc start from: {}", minOffset);
            } else {
                throw new CoreException("start failed");
            }
        } catch (Exception e) {
            throw new CoreException("Failed start cdc feed process, feed id: {}, message: {}", processInfo.feedId, e.getMessage(), e);
        }
    }

    protected void stopFeedProcess() {
        try(HttpUtil httpUtil = new HttpUtil(log)) {
            if (httpUtil.deleteChangefeed(processInfo.feedId, processInfo.cdcServer)) {
                log.info("Stop cdc succeed, feed id: {}, cdc server: {}", processInfo.feedId, processInfo.cdcServer);
            } else {
                throw new CoreException("Stop cdc failed, feed id: {}, cdc server: {}", processInfo.feedId, processInfo.cdcServer);
            }
        } catch (Exception e) {
            log.error("Failed to stop cdc feed process, feed id: {}, message: {}", processInfo.feedId, e.getMessage());
        }
    }

    public static class ProcessInfo {
        String database;
        String cdcPath;
        String hostPd;
        int portPd;
        String hostAddr;
        int portAddr;
        String hostAdvertiseAddr;
        int portAdvertiseAddr;
        String dataDir;
        String logFilePath;
        int gcTtl;
        List<String> cdcTable;
        AtomicReference<Throwable> throwableCollector;
        Supplier<Boolean> alive;
        TapConnectorContext nodeContext;
        Object cdcOffset;
        int batchSize;
        String feedId;
        String cdcServer;

        public ProcessInfo withCdcServer(String cdcServer) {
            this.cdcServer = cdcServer;
            return this;
        }

        public ProcessInfo withFeedId(String feedId) {
            this.feedId = feedId;
            return this;
        }

        public ProcessInfo withCdcBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ProcessInfo withTapConnectorContext(TapConnectorContext nodeContext) {
            this.nodeContext = nodeContext;
            return this;
        }

        public ProcessInfo withCdcOffset(Object cdcOffset) {
            this.cdcOffset = cdcOffset;
            return this;
        }

        public ProcessInfo withDatabase(String database) {
            this.database = database;
            return this;
        }

        public ProcessInfo withCdcPath(String cdcPath) {
            this.cdcPath = cdcPath;
            return this;
        }

        public ProcessInfo withPdHost(String hostPd) {
            this.hostPd = hostPd;
            return this;
        }

        public ProcessInfo withPdPort(int portPd) {
            this.portPd = portPd;
            return this;
        }

        public ProcessInfo withAddrHost(String hostAddr) {
            this.hostAddr = hostAddr;
            return this;
        }

        public ProcessInfo withAddrPort(int portAddr) {
            this.portAddr = portAddr;
            return this;
        }

        public ProcessInfo withAdvertiseAddrHost(String hostAdvertiseAddr) {
            this.hostAdvertiseAddr = hostAdvertiseAddr;
            return this;
        }

        public ProcessInfo withAdvertiseAddrPort(int portAdvertiseAddr) {
            this.portAdvertiseAddr = portAdvertiseAddr;
            return this;
        }

        public ProcessInfo withDataDir(String dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public ProcessInfo withLogFilePath(String logFilePath) {
            this.logFilePath = logFilePath;
            return this;
        }

        public ProcessInfo withGcTtl(int gcTtl) {
            this.gcTtl = gcTtl;
            return this;
        }

        public ProcessInfo withCdcTable(List<String> tableNames) {
            this.cdcTable = Optional.ofNullable(tableNames).orElse(new ArrayList<>());
            return this;
        }

        public ProcessInfo withThrowableCollector(AtomicReference<Throwable> throwableCollector) {
            this.throwableCollector = throwableCollector;
            return this;
        }

        public ProcessInfo withAlive(Supplier<Boolean> alive) {
            this.alive = alive;
            return this;
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public TapEventManager getTapEventManager() {
        return tapEventManager;
    }
}
