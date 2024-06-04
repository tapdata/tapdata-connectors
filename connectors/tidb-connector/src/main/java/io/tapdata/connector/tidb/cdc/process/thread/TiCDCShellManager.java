package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.cdc.util.ProcessLauncher;
import io.tapdata.connector.tidb.cdc.util.ProcessSearch;
import io.tapdata.connector.tidb.cdc.util.ResourcesLoader;
import io.tapdata.connector.tidb.cdc.util.ZipUtils;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TiCDCShellManager implements Activity {
    public static final Object PROCESS_LOCK = new Object();
    public static final String CDC_CMD = "${cdc_tool_path} server " +
            "--pd=${pd_ip_ports}  " +
            "--addr=${cdc_server_ip_port} " +
            "--cluster-id=${cluster_id} " +
            "--data-dir=${local_strong_path} " +
            "--gc-ttl=${gc_ttl} " +
            "--log-level=${log_level} " +
            "--log-file=${log_dir} ";

    final ShellConfig shellConfig;
    final Log log;
    public TiCDCShellManager(ShellConfig shellConfig) {
        this.shellConfig = shellConfig;
        this.log = shellConfig.context.getLog();
    }

    @Override
    public void init() {

    }

    @Override
    public void doActivity() {
        synchronized (PROCESS_LOCK) {
            String runningCdcInfo = getAllRunningCdcInfo();
            try {
                if (null == runningCdcInfo) {
                    loadCdcFile();
                    String[] command = ProcessSearch.getCommand(getCmdAfterSetProperties());

                    List<Integer> allCdcUsePort = getAllCdcUsePort();
                    if (allCdcUsePort.isEmpty()) {
                        shellConfig.withCdcServerIpPort("127.0.0.1:8300");
                    } else {
                        Integer port = allCdcUsePort.get(0);
                        shellConfig.withCdcServerIpPort(String.format("127.0.0.1:%d", port + 1));
                    }
                    ProcessLauncher.startProcessInBackground(command);
                } else {
                    shellConfig.withCdcServerIpPort(runningCdcInfo);
                }
            } finally {
                shellConfig.context.getStateMap().put("cdc-server", shellConfig.cdcServerIpPort);
                shellConfig.context.getStateMap().put("cdc-file-path", shellConfig.localStrongPath);
            }
        }
    }

    public void checkAlive() {
        try(HttpUtil util = new HttpUtil(log)) {
            if (util.queryChangefeedsList(shellConfig.cdcServerIpPort) <= 0) {
                doActivity();
            }
        } catch (Exception e) {
            log.debug("Unable query TiDB cdc process, server: {}, message: {}", shellConfig.cdcServerIpPort, e.getMessage());
        }
    }

    protected String setProperties(String cmd, String key, Object value) {
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
        cmd = setProperties(cmd, "gc_ttl", shellConfig.gcTtl);
        cmd = setProperties(cmd, "log_level", shellConfig.logLevel.name);
        cmd = setProperties(cmd, "log_dir", new File(shellConfig.logDir).getAbsolutePath());
        log.info(cmd);
        return cmd;
    }

    public String getCdcToolPath() {
        String cdcToolName = cdcFileName();
        String toolPath = FileUtil.paths("run-resources", "ti-db", "tool");
        return new File(FileUtil.paths(toolPath, cdcToolName)).getAbsolutePath();
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    public String getAllRunningCdcInfo() {
        List<String> server = ProcessSearch.getProcesses(log,
                getCdcToolPath(),
                shellConfig.cdcServerIpPort,
                "server",
                "--pd=",
                "--addr=",
                "--data-dir=",
                "--gc-ttl=",
                "-L=");
        if (!server.isEmpty()) {
            String cmdLine = server.get(0);
            return getInfoFromCmdLine(cmdLine, "--addr=\"", "\" ");
        }
        return null;
    }

    public List<Integer> getAllCdcUsePort() {
        List<String> server = ProcessSearch.getProcesses(log,
                getCdcToolPath(),
                "server",
                "--pd=",
                "--addr=",
                "--data-dir=",
                "--gc-ttl=",
                "-L=");
        List<Integer> allCdcPort = new ArrayList<>();
        if (!server.isEmpty()) {
            for (String cmdLine : server) {
                allCdcPort.add(getCdcServerPort(getInfoFromCmdLine(cmdLine, "--addr=\"", "\" ")));
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
        String cdcToolName = cdcFileName();
        File file = new File(FileUtil.paths(toolPath, cdcToolName));
        if (!file.exists() || !file.isFile()) {
            String zipName = FileUtil.paths("ti-cdc", cdcToolName + ".zip");
            try {
                ResourcesLoader.unzipSources(zipName, toolPath, log);
            } catch (CoreException e) {
                if (e.getCode() == ResourcesLoader.ZIP_NOT_EXISTS) {
                    ZipUtils.unzip(FileUtil.paths(toolPath, zipName), toolPath);
                    return;
                }
                throw e;
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
        } else if(osArch.contains("x86_64")) {
            return "cdc_x86_64";
        } else if (osArch.contains("x86")) {
            //This machine is running on an x86 architecture
            return "cdc_x86";
        } else {
            return "cdc";
        }
    }

    public String getDatabaseTag() {
        return String.format("%s.%d", shellConfig.tidbConfig.getHost(), shellConfig.tidbConfig.getPort());
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
                URI uri = URI.create(pdIpPorts);
                String protocol = uri.toURL().getProtocol();
                if (StringUtils.isBlank(protocol)) {
                    pdIpPorts = "http://" + pdIpPorts;
                }
            } catch (Exception e) {
                throw new CoreException("PD server is illegal, should start with a protocol, such as: or https://${ip:port}, error chars: {}", pdIpPorts);
            }
            this.pdIpPorts = pdIpPorts;
            return this;
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
        INFO("info"), DEBUG("debug"),WARN("warn"),ERROR("error"),FATAL("fatal");
        String name;
        LogLevel(String n) {
            this.name = n;
        }
    }
}
