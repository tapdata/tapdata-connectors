package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.cdc.util.AvailablePorts;
import io.tapdata.connector.tidb.cdc.util.ProcessLauncher;
import io.tapdata.connector.tidb.cdc.util.ProcessSearch;
import io.tapdata.connector.tidb.cdc.util.ResourcesLoader;
import io.tapdata.connector.tidb.cdc.util.ZipUtils;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TiCDCShellManager implements Activity {
    public static final int CDC_TOOL_NOT_EXISTS = 1000010;
    public static final Object PROCESS_LOCK = new Object();
    public static final String CDC_CMD = "${cdc_tool_path} server " +
            "--pd=${pd_ip_ports} " +
            "--addr=${cdc_server_ip_port} " +
            "--cluster-id=${cluster_id} " +
            "--data-dir=${local_strong_path} " +
            "--log-level=${log_level} " +
            "--log-file=${log_dir} ";

    public static String getCdcPsGrepFilter(String pdServer, String cdcServer) {
        String filter = setProperties("' server --pd=${pd_ip_ports} --addr=${cdc_server_ip_port} '", "pd_ip_ports", pdServer);
        return setProperties(filter, "cdc_server_ip_port", cdcServer);
    }

    public static String getPdServerGrepFilter(String pdServer) {
        return setProperties("' server --pd=${pd_ip_ports} '", "pd_ip_ports", pdServer);
    }

    final ShellConfig shellConfig;
    final Log log;

    public TiCDCShellManager(ShellConfig shellConfig) {
        this.shellConfig = shellConfig;
        this.log = shellConfig.context.getLog();
    }

    @Override
    public void init() {
        //do nothing
    }

    protected void checkDir(String path) {
        File logDir = new File(path);
        Path toPath = logDir.toPath();
        try {
            if (!java.nio.file.Files.exists(toPath)) {
                java.nio.file.Files.createDirectories(toPath);
                log.debug("TiCDC file dir not exists, make dir succeed: {}", logDir.getAbsolutePath());
                return;
            }
            if (!java.nio.file.Files.isDirectory(toPath)) {
                java.nio.file.Files.delete(toPath);
                java.nio.file.Files.createDirectories(toPath);
                log.debug("TiCDC file dir is file, after delete this file make dir succeed: {}", logDir.getAbsolutePath());
            }
        } catch (IOException e) {
            log.debug("TiCDC file dir is file, after delete this file make dir failed: {}", logDir.getAbsolutePath());
        }
    }

    @Override
    public void doActivity() {
        checkDir(BASE_CDC_LOG_DIR);
        checkDir(new File(shellConfig.localStrongPath).getAbsolutePath());
        synchronized (PROCESS_LOCK) {
            String runningCdcInfo = getAllRunningCdcInfo();
            try {
                if (null == runningCdcInfo) {
                    loadCdcFile();
                    List<Integer> allCdcUsePort = getAllCdcUsePort();
                    int availablePort = 8300;
                    if (!allCdcUsePort.isEmpty()) {
                        availablePort = allCdcUsePort.get(0) + 1;
                    }
                    shellConfig.withCdcServerIpPort(String.format("127.0.0.1:%d", AvailablePorts.getAvailable(availablePort)));
                    String[] command = ProcessSearch.getCommand(getCmdAfterSetProperties());
                    ProcessLauncher.startProcessInBackground(command);
                    checkAliveAndWait();
                } else {
                    shellConfig.withCdcServerIpPort(runningCdcInfo);
                }
            } finally {
                shellConfig.context.getStateMap().put(ProcessHandler.CDC_SERVER, shellConfig.cdcServerIpPort);
            }
        }
    }

    protected void checkAliveAndWait() {
        int waitTimes = 5;
        try (HttpUtil httpUtil = HttpUtil.of(log)) {
            while (!httpUtil.checkAlive(shellConfig.cdcServerIpPort)) {
                if (waitTimes < 0) {
                    log.warn("After five second, TiCDC server not alive, server: {}", shellConfig.cdcServerIpPort);
                    break;
                }
                try {
                    //sleep 1s to wait start process complete
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                waitTimes--;
            }
            log.info("TiCDC server start succeed, server: {}", shellConfig.cdcServerIpPort);
        }
    }

    public void checkAlive() {
        try (HttpUtil util = HttpUtil.of(log)) {
            if (!util.checkAlive(shellConfig.cdcServerIpPort)) {
                doActivity();
            }
        } catch (Exception e) {
            log.debug("Unable check TiDB cdc process alive, server: {}, message: {}", shellConfig.cdcServerIpPort, e.getMessage());
        }
    }

    public static String setProperties(String cmd, String key, Object value) {
        String keyReg = "${" + key + "}";
        while (cmd.contains(keyReg)) {
            cmd = cmd.replace(keyReg, String.valueOf(value));
        }
        return cmd;
    }

    protected String getCmdAfterSetProperties() {
        String cmd = setProperties(CDC_CMD, "cdc_tool_path", getCdcToolPath());
        cmd = setProperties(cmd, "pd_ip_ports", shellConfig.pdIpPorts);
        cmd = setProperties(cmd, "cdc_server_ip_port", shellConfig.cdcServerIpPort);
        cmd = setProperties(cmd, "cluster_id", shellConfig.clusterId);
        cmd = setProperties(cmd, "local_strong_path", new File(shellConfig.localStrongPath).getAbsolutePath());
        cmd = setProperties(cmd, "log_level", shellConfig.logLevel.name);
        cmd = setProperties(cmd, "log_dir", new File(shellConfig.logDir).getAbsolutePath());
        log.info(cmd);
        return cmd;
    }

    public String getCdcToolPath() {
        return new File(FileUtil.paths("run-resources", "ti-db", "tool", "cdc")).getAbsolutePath();
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    public String getAllRunningCdcInfo() {
        List<String> server = ProcessSearch.getProcesses(log, getPdServerGrepFilter(shellConfig.pdIpPorts));
        if (!server.isEmpty()) {
            String cmdLine = server.get(0);
            return getInfoFromCmdLine(cmdLine, "--addr=", " ");
        }
        return null;
    }

    public List<Integer> getAllCdcUsePort() {
        List<String> server = ProcessSearch.getProcesses(log, getCdcToolPath());
        List<Integer> allCdcPort = new ArrayList<>();
        if (!server.isEmpty()) {
            for (String cmdLine : server) {
                if (cmdLine.contains("--addr=")) {
                    allCdcPort.add(getCdcServerPort(getInfoFromCmdLine(cmdLine, "--addr=", " ")));
                }
            }
            allCdcPort.sort(Comparator.reverseOrder());
        }
        return allCdcPort;
    }

    protected String getInfoFromCmdLine(String cmdLine, String pix, String suf) {
        int index = cmdLine.indexOf(pix);
        int boundIndex = cmdLine.indexOf(suf, index);
        return cmdLine.substring(index + pix.length(), boundIndex);
    }

    protected int getCdcServerPort(String cdcServer) {
        String[] split = cdcServer.split(":");
        if (split.length == 2) {
            try {
                return Integer.parseInt(split[1]);
            } catch (Exception e) {
                return 8300;
            }
        }
        return 8300;
    }

    protected void loadCdcFile() {
        String toolPath = FileUtil.paths("run-resources", "ti-db", "tool");
        File toolDir = new File(toolPath);
        if ((!toolDir.exists() || !toolDir.isDirectory()) && !toolDir.mkdirs()) {
            log.warn("Can not make cdc work dir: {}", toolDir.getAbsolutePath());
        }
        String cdcToolName = cdcFileName();
        File file = new File(getCdcToolPath());
        if (!file.exists() || !file.isFile()) {
            log.info("File {} does not exist, resource files will be found from the default configuration based on system architecture or custom directories",
                    file.getAbsolutePath());
            String zipName = FileUtil.paths("ti-cdc", cdcToolName + ".zip");
            try {
                ResourcesLoader.unzipSources(zipName, toolPath, log);
            } catch (CoreException e) {
                if (e.getCode() != ResourcesLoader.ZIP_NOT_EXISTS) {
                    throw new CoreException(CDC_TOOL_NOT_EXISTS, e.getMessage());
                }
                String paths = FileUtil.paths(toolPath, zipName);
                log.info("No available TiCDC resources found, sources name: {}, going to directory {} to match custom resources soon",
                        zipName, paths);
                File f = new File(paths);
                if (f.exists() || !f.isFile()) {
                    log.warn("File {} does not exist, please manually add it", f.getAbsolutePath());
                } else {
                    try {
                        ZipUtils.unzip(paths, toolPath);
                    } catch (Exception e1) {
                        log.warn("Unzip custom resources filed, message: {}", e1.getMessage());
                    }
                }
            }
            File cdcTool = new File(getCdcToolPath());
            if (!cdcTool.exists() || !cdcTool.isFile()) {
                log.error("TiCDC must not start normally, TiCDC server depends on {}, make sure this file in you file system", cdcTool.getAbsolutePath());
            }
        }
    }

    protected String cdcFileName() {
        String osArch = System.getProperty("os.arch").toLowerCase();
        if (osArch.contains("arm")) {
            //"This machine is running on an ARM architecture
            return "cdc_arm";
        } else if (osArch.contains("amd64")) {
            //This machine is running on an x86-64 (AMD64) architecture
            return "cdc_amd";
        } else if (osArch.contains("x86_64")) {
            return "cdc_x86_64";
        } else if (osArch.contains("x86")) {
            //This machine is running on an x86 architecture
            return "cdc_x86";
        } else {
            return "cdc";
        }
    }

    public static class ShellConfig {
        TidbConfig tidbConfig;
        String clusterId;
        String pdIpPorts;
        String cdcServerIpPort;
        String localStrongPath;
        int gcTtl = 86400;
        String logDir;
        LogLevel logLevel = LogLevel.INFO;
        TapConnectorContext context;

        public ShellConfig withTapConnectionContext(TapConnectorContext context) {
            this.context = context;
            return this;
        }

        public ShellConfig withTiDBConfig(TidbConfig config) {
            this.tidbConfig = config;
            return this;
        }

        public ShellConfig withClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public ShellConfig withPdIpPorts(String pdIpPorts) {
            try {
                pdIpPorts = pdIpPorts.startsWith("http") ?
                        pdIpPorts
                        : "http://" + pdIpPorts;
            } catch (Exception e) {
                throw new CoreException("PD server is illegal, should start with a protocol, such as: or https://${ip:port}, error chars: {}", pdIpPorts);
            }
            this.pdIpPorts = pdIpPorts;
            return this;
        }

        public String getPdIpPorts() {
            return this.pdIpPorts;
        }

        public ShellConfig withCdcServerIpPort(String cdcServerIpPort) {
            this.cdcServerIpPort = cdcServerIpPort;
            return this;
        }

        public ShellConfig withLocalStrongPath(String localStrongPath) {
            this.localStrongPath = localStrongPath;
            return this;
        }

        public ShellConfig withGcTtl(int gcTtl) {
            this.gcTtl = gcTtl;
            return this;
        }

        public ShellConfig withLogDir(String logDir) {
            this.logDir = logDir;
            return this;
        }

        public ShellConfig withLogLevel(LogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }
    }

    public enum LogLevel {
        INFO("info"), DEBUG("debug"), WARN("warn"), ERROR("error"), FATAL("fatal");
        String name;

        LogLevel(String n) {
            this.name = n;
        }
    }
}
