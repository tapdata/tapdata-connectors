package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.common.util.FileUtil;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;

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
    public static final String BASE_CDC_DATA_DIR = "/Users/xiao/Documents/GitHub/kit/tidb/json";//"ti-db/cdc";
    public final Object tableVersionLock = new Object();
    final ProcessInfo processInfo;
    final ConcurrentHashMap<String, String> tableVersionMap;

    final DDLManager ddlManager;
    final DMLManager dmlManager;
    final TapEventManager tapEventManager;
    public ProcessHandler(ProcessInfo processInfo, StreamReadConsumer consumer) {
        this.processInfo = processInfo;
        this.tableVersionMap = new ConcurrentHashMap<>();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.tapEventManager = new TapEventManager(this,500, consumer);
        this.ddlManager = new DDLManager(this, BASE_CDC_DATA_DIR);
        this.dmlManager = new DMLManager(this, BASE_CDC_DATA_DIR, 3);
    }

    protected String judgeTableVersion(String tableName) {
        String tableVersion = null;
        synchronized (tableVersionLock) {
            if (tableVersionMap.containsKey(tableName)
                    && null != (tableVersion = tableVersionMap.get(tableName))) {
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
            //@todo
        }
    }

    protected void closeOnce(Activity activity) {
        Optional.ofNullable(activity).ifPresent(a -> {
            try {
                a.close();
            } catch (Exception e) {
                //@todo
            }
        });
    }

    @Override
    public void init() {
        this.ddlManager.init();
        this.dmlManager.init();
        this.tapEventManager.init();
    }

    @Override
    public void doActivity() {
        this.ddlManager.doActivity();
        this.dmlManager.doActivity();
        this.tapEventManager.doActivity();
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
