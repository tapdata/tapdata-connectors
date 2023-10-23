package io.tapdata.connector.postgres.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.util.FileUtil;
import io.tapdata.kit.EmptyKit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
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

    //customize
    public PostgresConfig() {
        setDbType("postgresql");
        setJdbcDriver("org.postgresql.Driver");
    }

    public void generateSSlFile() throws IOException {
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
            String sslCaPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCa()), sslCaPath, true);
            properties.put("sslrootcert", FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.pem"));
        }
        //如果客户端证书有内容，表示需要验证客户端证书，导入keystore.jks
        if (EmptyKit.isNotBlank(getSslCert()) && EmptyKit.isNotBlank(getSslKey())) {
            String sslCertPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCert()), sslCertPath, true);
            String sslKeyPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslKey()), sslKeyPath, true);
            properties.put("sslcert", FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem"));
            properties.put("sslkey", FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem"));
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
}
