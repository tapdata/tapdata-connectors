package io.tapdata.connector.tidb.cdc.process.thread;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.cdc.util.ReplaceUtil;
import io.tapdata.connector.tidb.config.TidbConfig;
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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public final class ProcessHandler implements Activity {
    public static final String CDC_FILE_PATH = "cdc-file-path";
    public static final String FEED_ID = "feed-id";
    public static final String CDC_SERVER = "cdc-server";
    private final ScheduledExecutorService scheduledExecutorService;
    public final Object tableVersionLock = new Object();
    final ProcessInfo processInfo;
    final ConcurrentHashMap<String, DDLManager.VersionInfo> tableVersionMap;

    final DDLManager ddlManager;
    final DMLManager dmlManager;
    final TiCDCShellManager shellManager;
    final TapEventManager tapEventManager;
    final Log log;
    final String basePath;

    public static ProcessHandler of(ProcessInfo processInfo, StreamReadConsumer consumer) {
        return new ProcessHandler(processInfo, consumer);
    }

    public ProcessHandler(ProcessInfo processInfo, StreamReadConsumer consumer) {
        this.processInfo = processInfo;
        this.tableVersionMap = new ConcurrentHashMap<>();
        this.log = processInfo.nodeContext.getLog();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.tapEventManager = new TapEventManager(this, consumer);
        String pdServerPath = pdServerPath(processInfo.tidbConfig.getPdServer());
        this.basePath = String.format(BASE_CDC_DATA_DIR, pdServerPath, processInfo.feedId);
        processInfo.nodeContext.getStateMap().put(ProcessHandler.CDC_FILE_PATH, basePath);
        this.ddlManager = new DDLManager(this, basePath);
        int threadCount = 5;
        if (CollectionUtils.isNotEmpty(processInfo.cdcTable)) {
            int tableCount = processInfo.cdcTable.size();
            threadCount = tableCount / 5 + (tableCount % 5 > 0 ? 1 : 0);
            if (threadCount > 5) {
                threadCount = 5;
            }
        }
        this.dmlManager = new DMLManager(this, basePath, threadCount);
        this.shellManager = new TiCDCShellManager(new TiCDCShellManager.ShellConfig()
                .withCdcServerIpPort(processInfo.cdcServer)
                .withGcTtl(processInfo.gcTtl)
                .withLocalStrongPath(FileUtil.paths(BASE_CDC_CACHE_DATA_DIR, pdServerPath))
                .withPdIpPorts(processInfo.tidbConfig.getPdServer())
                .withLogDir(FileUtil.paths(BASE_CDC_LOG_DIR, pdServerPath + "_" + System.currentTimeMillis() + ".log"))
                .withLogLevel(TiCDCShellManager.LogLevel.INFO)
                .withClusterId(UUID.randomUUID().toString().replace("-", ""))
                .withTapConnectionContext(processInfo.nodeContext)
                .withTiDBConfig(processInfo.tidbConfig));
    }

    public static String pdServerPath(String pdServer) {
        try {
            URL url = new URL(pdServer);
            return String.format("%s_%s", ReplaceUtil.replaceAll(url.getHost(), ".", "_"), url.getPort());
        } catch (Exception e) {
            return "temp";
        }
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
        this.ddlManager.init();
        this.dmlManager.init();
    }

    @Override
    public void doActivity() {
        init();
        shellManager.doActivity();
        processInfo.withCdcServer(shellManager.shellConfig.cdcServerIpPort);
        stopFeedProcess();
        startFeedProcess();
        this.ddlManager.doActivity();
        this.dmlManager.doActivity();
    }

    public void aliveCheck() {
        shellManager.checkAlive();
    }

    protected void startFeedProcess() {
        try (HttpUtil httpUtil = HttpUtil.of(log)) {
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
            checkAliveAndWait(httpUtil, changefeed, minOffset);
        } catch (Exception e) {
            throw new CoreException("Failed start cdc feed process, feed id: {}, message: {}", processInfo.feedId, e.getMessage(), e);
        }
    }

    protected void createChangeFeed(HttpUtil httpUtil, ChangeFeed changefeed, Long minOffset) throws IOException {
        if (httpUtil.createChangeFeed(changefeed, processInfo.cdcServer)) {
            log.info("TiCDC start successfully from: {}", minOffset);
        } else {
            throw new CoreException("TiCDC server start change feed failed, server: {}, change feed id: {}", processInfo.cdcServer, changefeed);
        }
    }

    protected void checkAliveAndWait(HttpUtil httpUtil, ChangeFeed changefeed, Long minOffset) throws IOException {
        int waitTimes = 5;
        do {
            if (waitTimes < 0) {
                log.warn("After five second, Ti change feed not alive, server: {}, feed id: {}",
                        shellManager.shellConfig.cdcServerIpPort,
                        processInfo.feedId
                );
                break;
            }
            createChangeFeed(httpUtil, changefeed, minOffset);
            sleep();
            waitTimes--;
        } while (!httpUtil.queryChangeFeedsList(shellManager.shellConfig.cdcServerIpPort, processInfo.feedId));
    }

    protected void sleep() {
        try {
            //sleep 1s to wait start process complete
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void stopFeedProcess() {
        try (HttpUtil httpUtil = HttpUtil.of(log)) {
            if (!httpUtil.checkAlive(processInfo.cdcServer)) {
                return;
            }
            if (Boolean.TRUE.equals(httpUtil.deleteChangeFeed(processInfo.feedId, processInfo.cdcServer))) {
                log.debug("Stop cdc succeed, feed id: {}, cdc server: {}", processInfo.feedId, processInfo.cdcServer);
            } else {
                throw new CoreException("Stop cdc failed, feed id: {}, cdc server: {}", processInfo.feedId, processInfo.cdcServer);
            }
        } catch (Exception e) {
            log.warn("Failed to stop cdc feed process, feed id: {}, message: {}", processInfo.feedId, e.getMessage());
        }
    }

    public static class ProcessInfo {
        String database;
        int gcTtl;
        List<String> cdcTable;
        AtomicReference<Throwable> throwableCollector;
        BooleanSupplier alive;
        TapConnectorContext nodeContext;
        Object cdcOffset;
        String feedId;
        String cdcServer;
        TidbConfig tidbConfig;
        TimeZone timezone;
        public ProcessInfo withZone(TimeZone timezone) {
            this.timezone = timezone;
            return this;
        }

        public ProcessInfo withTiDBConfig(TidbConfig tidbConfig) {
            this.tidbConfig = tidbConfig;
            return this;
        }

        public ProcessInfo withCdcServer(String cdcServer) {
            this.cdcServer = cdcServer;
            return this;
        }

        public ProcessInfo withFeedId(String feedId) {
            this.feedId = feedId;
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

        public ProcessInfo withAlive(BooleanSupplier alive) {
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
