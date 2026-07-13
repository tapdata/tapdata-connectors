package io.tapdata.connector.postgres.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.util.FileUtil;
import io.tapdata.kit.EmptyKit;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class PostgresConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version
    private Boolean closeNotNull = false;
    private Boolean allowReplication = true;
    private String tableOwner = "";
    private String replaceBlank = "";
    private Boolean partitionRoot = false;
    private Integer maximumQueueSize = 8000;
    private List<String> distributedKey = new ArrayList<>();
    private Boolean isPartition = false;
    private String customSlotName;
    private String pgtoHost = "127.0.0.1";
    private int pgtoPort = 9876;
    private Integer defaultWalLogSize = 102400;
    private Boolean partPublication = false;
    private String globalPublicationName = "dbz_publication";
    private String customPublicationName = "";

    private String deploymentMode;
    private ArrayList<LinkedHashMap<String, Integer>> masterSlaveAddress;

    // DDL trigger (Attunity-style event trigger for capturing DDL via WAL)
    private Boolean ddlTriggerEnable = false;
    private String ddlTriggerSchema;
    private Integer keepWalHours = 0;
    private Boolean autoClearSlot = true;

    // ── Physical WAL miner tuning ──
    // Per-task switch for verbose [WAL-DEBUG] logging; falls back to env TAPDATA_WAL_DEBUG
    private Boolean walDebug;
    // Page-state cache capacity (0 = unbounded); falls back to env TAPDATA_WAL_PAGE_CACHE_CAPACITY
    private Integer walPageCacheCapacity;
    // Per-xid spill-to-disk threshold in rows (default 500K)
    private Integer walSpillThreshold;
    // Custom spill directory; falls back to env TAPDATA_WAL_SPILL_DIR or java.io.tmpdir
    private String walSpillDir;
    // Number of WAL segments to look back for cold-cache warm-up (default 10)
    private Integer walLookbackSegments;

    //customize
    public PostgresConfig() {
        setDbType("postgresql");
        setJdbcDriver("org.postgresql.Driver");
        setMaxIndexNameLength(63);
        Properties properties = new Properties();
        properties.put("stringtype", "unspecified");
        properties.put("prepareThreshold", "0");
        setProperties(properties);
    }

    public String getConnectionString() {
        if ("master-slave".equals(deploymentMode)) {
            return masterSlaveAddress.stream().map(v -> v.get("host") + ":" + v.get("port")).collect(Collectors.joining(",")) + "/" + getDatabase() + "/" + getSchema();
        }
        return super.getConnectionString();
    }

    @Override
    public void generateSSlFile() throws IOException, InterruptedException {
        //SSL开启需要的URL属性
        properties.put("ssl", "true");
        //每个config都用随机路径
        sslRandomPath = UUID.randomUUID().toString().replace("-", "");
        //如果所有文件都没有上传，表示不验证证书，直接结束
        if (EmptyKit.isBlank(getSslCa()) && EmptyKit.isBlank(getSslCert()) && EmptyKit.isBlank(getSslKey())) {
            return;
        }
        //如果CA证书有内容，表示需要验证CA证书
        if (EmptyKit.isNotBlank(getSslCa())) {
            properties.setProperty("sslmode", "verify-ca");
            String sslCaPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.crt");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCa()), sslCaPath, true);
            properties.put("sslrootcert", FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.crt"));
        }
        //如果客户端证书有内容，表示需要验证客户端证书，导入keystore.jks
        if (EmptyKit.isNotBlank(getSslCert()) && EmptyKit.isNotBlank(getSslKey())) {
            String sslCertPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client.crt");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCert()), sslCertPath, true);
            String sslKeyPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client.key");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslKey()), sslKeyPath, true);
            Runtime.getRuntime().exec("openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt -in " +
                    FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client.key") + " -out " +
                    FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client.pk8")).waitFor();
            properties.put("sslcert", FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client.crt"));
            properties.put("sslkey", FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client.pk8"));
        }
        if (EmptyKit.isNotBlank(getSslKeyPassword())) {
            properties.setProperty("sslpassword", getSslKeyPassword());
        }
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }

    public Boolean getCloseNotNull() {
        return closeNotNull;
    }

    public void setCloseNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
    }

    public Boolean getAllowReplication() {
        return allowReplication;
    }

    public void setAllowReplication(Boolean allowReplication) {
        this.allowReplication = allowReplication;
    }

    public String getTableOwner() {
        return tableOwner;
    }

    public void setTableOwner(String tableOwner) {
        this.tableOwner = tableOwner;
    }

    public String getReplaceBlank() {
        return replaceBlank;
    }

    public void setReplaceBlank(String replaceBlank) {
        this.replaceBlank = replaceBlank;
    }

    public Boolean getPartitionRoot() {
        return partitionRoot;
    }

    public void setPartitionRoot(Boolean partitionRoot) {
        this.partitionRoot = partitionRoot;
    }

    public Integer getMaximumQueueSize() {
        return maximumQueueSize;
    }

    public void setMaximumQueueSize(Integer maximumQueueSize) {
        this.maximumQueueSize = maximumQueueSize;
    }

    public List<String> getDistributedKey() {
        return distributedKey;
    }

    public void setDistributedKey(List<String> distributedKey) {
        this.distributedKey = distributedKey;
    }

    public Boolean getIsPartition() {
        return isPartition;
    }

    public void setIsPartition(Boolean isPartition) {
        this.isPartition = isPartition;
    }

    public String getPgtoHost() {
        return pgtoHost;
    }

    public String getCustomSlotName() {
        return customSlotName;
    }

    public void setCustomSlotName(String customSlotName) {
        this.customSlotName = customSlotName;
    }

    public void setPgtoHost(String pgtoHost) {
        this.pgtoHost = pgtoHost;
    }

    public int getPgtoPort() {
        return pgtoPort;
    }

    public void setPgtoPort(int pgtoPort) {
        this.pgtoPort = pgtoPort;
    }

    public Integer getDefaultWalLogSize() {
        return defaultWalLogSize;
    }

    public void setDefaultWalLogSize(Integer defaultWalLogSize) {
        this.defaultWalLogSize = defaultWalLogSize;
    }

    public Boolean getPartPublication() {
        return partPublication;
    }

    public void setPartPublication(Boolean partPublication) {
        this.partPublication = partPublication;
    }

    public String getGlobalPublicationName() {
        return globalPublicationName;
    }

    public void setGlobalPublicationName(String globalPublicationName) {
        this.globalPublicationName = globalPublicationName;
    }

    public String getCustomPublicationName() {
        return customPublicationName;
    }

    public void setCustomPublicationName(String customPublicationName) {
        this.customPublicationName = customPublicationName;
    }

    public String getDeploymentMode() {
        return deploymentMode;
    }

    public void setDeploymentMode(String deploymentMode) {
        this.deploymentMode = deploymentMode;
    }

    public ArrayList<LinkedHashMap<String, Integer>> getMasterSlaveAddress() {
        return masterSlaveAddress;
    }

    public void setMasterSlaveAddress(ArrayList<LinkedHashMap<String, Integer>> masterSlaveAddress) {
        this.masterSlaveAddress = masterSlaveAddress;
    }

    public Boolean getDdlTriggerEnable() {
        return ddlTriggerEnable;
    }

    public void setDdlTriggerEnable(Boolean ddlTriggerEnable) {
        this.ddlTriggerEnable = ddlTriggerEnable;
    }

    public String getDdlTriggerSchema() {
        return ddlTriggerSchema;
    }

    public void setDdlTriggerSchema(String ddlTriggerSchema) {
        this.ddlTriggerSchema = ddlTriggerSchema;
    }

    public Integer getKeepWalHours() {
        return keepWalHours;
    }

    public void setKeepWalHours(Integer keepWalHours) {
        this.keepWalHours = keepWalHours;
    }

    public Boolean getAutoClearSlot() {
        return autoClearSlot;
    }

    public void setAutoClearSlot(Boolean autoClearSlot) {
        this.autoClearSlot = autoClearSlot;
    }

    // ── Physical WAL miner accessors ──

    public Boolean getWalDebug() {
        return walDebug;
    }

    public void setWalDebug(Boolean walDebug) {
        this.walDebug = walDebug;
    }

    public Integer getWalPageCacheCapacity() {
        return walPageCacheCapacity;
    }

    public void setWalPageCacheCapacity(Integer walPageCacheCapacity) {
        this.walPageCacheCapacity = walPageCacheCapacity;
    }

    public Integer getWalSpillThreshold() {
        return walSpillThreshold;
    }

    public void setWalSpillThreshold(Integer walSpillThreshold) {
        this.walSpillThreshold = walSpillThreshold;
    }

    public String getWalSpillDir() {
        return walSpillDir;
    }

    public void setWalSpillDir(String walSpillDir) {
        this.walSpillDir = walSpillDir;
    }

    public Integer getWalLookbackSegments() {
        return walLookbackSegments;
    }

    public void setWalLookbackSegments(Integer walLookbackSegments) {
        this.walLookbackSegments = walLookbackSegments;
    }
}
