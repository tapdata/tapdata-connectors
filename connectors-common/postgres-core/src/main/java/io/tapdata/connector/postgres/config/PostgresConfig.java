package io.tapdata.connector.postgres.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.util.FileUtil;
import io.tapdata.kit.EmptyKit;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class PostgresConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version
    private Boolean closeNotNull = false;
    private String tableOwner = "";
    private String replaceBlank = "";
    private Boolean partitionRoot = false;
    private Integer maximumQueueSize = 8000;
    private List<String> distributedKey = new ArrayList<>();
    private Boolean isPartition = false;
    private String pgtoHost;
    private int pgtoPort;
    private String customSlotName;

    //customize
    public PostgresConfig() {
        setDbType("postgresql");
        setJdbcDriver("org.postgresql.Driver");
        setMaxIndexNameLength(63);
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
}
