package io.tapdata.common;

import io.tapdata.common.util.FileUtil;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * common relation database config
 *
 * @author Jarad
 * @date 2022/5/30
 */
public class CommonDbConfig implements Serializable {

    protected static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util
    protected static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String __connectionType; //target or source, see ConnectionTypeEnum
    private String dbType; //db protocol, set it when init
    private String host;
    private int port;
    private String database;
    private String schema;
    private String user;
    private String password;
    private String extParams;
    private String jdbcDriver;
    protected Properties properties;
    private char escapeChar = '"';

    private Boolean useSSL = false;
    private String sslCa;
    private String sslCert;
    private String sslKey;
    private String sslKeyPassword;
    private String sslRandomPath;

    //pattern for jdbc-url
    public String getDatabaseUrlPattern() {
        // last %s reserved for extend params
        return "jdbc:" + dbType + "://%s:%d/%s%s";
    }

    public String getConnectionString() {
        String connectionString = host + ":" + port + "/" + database;
        if (EmptyKit.isNotBlank(schema)) {
            connectionString += "/" + schema;
        }
        return connectionString;
    }

    //deal with extend params no matter there is ?
    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?") && !this.getExtParams().startsWith(":")) {
            this.setExtParams("?" + this.getExtParams());
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public CommonDbConfig load(String json) {
        try {
            assert beanUtils != null;
            assert jsonParser != null;
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            return this;
        } catch (Exception e) {
            throw new IllegalArgumentException("json string is not valid for db config");
        }
    }

    /**
     * load config from map, then need to cast it into its own class
     *
     * @param map attributes for config
     * @return ? extends CommonDbConfig
     */
    public CommonDbConfig load(Map<String, Object> map) {
        properties = new Properties();
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        if (useSSL && EmptyKit.isNotEmpty(map) && map.containsKey("useSSL")) {
            try {
                generateSSlFile();
            } catch (Exception e) {
                throw new IllegalArgumentException("generate ssl file failed");
            }
        }
        return this;
    }

    public CommonDbConfig copy() throws Exception {
        assert beanUtils != null;
        CommonDbConfig newConfig = this.getClass().newInstance();
        beanUtils.copyProperties(this, newConfig);
        return newConfig;
    }

    public void generateSSlFile() throws IOException, InterruptedException {
        //SSL开启需要的URL属性
        properties.put("useSSL", "true");
        properties.put("requireSSL", "true");
        //每个config都用随机路径
        sslRandomPath = UUID.randomUUID().toString().replace("-", "");
        //如果所有文件都没有上传，表示不验证证书，直接结束
        if (EmptyKit.isBlank(getSslCa()) && EmptyKit.isBlank(getSslCert()) && EmptyKit.isBlank(getSslKey())) {
            properties.put("verifyServerCertificate", "false");
            return;
        }
        properties.put("verifyServerCertificate", "true");
        //如果CA证书有内容，表示需要验证CA证书，导入truststore.jks
        if (EmptyKit.isNotBlank(getSslCa())) {
            String sslCaPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCa()), sslCaPath, true);
            Runtime.getRuntime().exec("keytool -import -noprompt -file " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.pem") +
                    " -keystore " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "truststore.jks") + " -storepass 123456").waitFor();
            properties.put("trustCertificateKeyStoreUrl", "file:" + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "truststore.jks"));
            properties.put("trustCertificateKeyStorePassword", "123456");
        }
        //如果客户端证书有内容，表示需要验证客户端证书，导入keystore.jks
        if (EmptyKit.isNotBlank(getSslCert()) && EmptyKit.isNotBlank(getSslKey())) {
            String sslCertPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCert()), sslCertPath, true);
            String sslKeyPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslKey()), sslKeyPath, true);
            Runtime.getRuntime().exec("openssl pkcs12 -legacy -export -in " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem") +
                    " -inkey " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem") +
                    " -name datasource-client -passout pass:123456 -out " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client-keystore.p12")).waitFor();
            Runtime.getRuntime().exec("keytool -importkeystore -srckeystore " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client-keystore.p12") +
                    " -srcstoretype pkcs12 -srcstorepass 123456 -destkeystore " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "keystore.jks") + " -deststoretype JKS -deststorepass 123456").waitFor();
            properties.put("clientCertificateKeyStoreUrl", "file:" + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "keystore.jks"));
            properties.put("clientCertificateKeyStorePassword", "123456");
        }
    }

    public void deleteSSlFile() {
        if (useSSL && EmptyKit.isNotBlank(sslRandomPath)) {
            File cacheDir = new File(FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath));
            if (cacheDir.exists()) {
                ErrorKit.ignoreAnyError(() -> FileUtils.deleteDirectory(cacheDir));
            }
        }
    }

    public String get__connectionType() {
        return __connectionType;
    }

    public void set__connectionType(String __connectionType) {
        this.__connectionType = __connectionType;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getExtParams() {
        return extParams;
    }

    public void setExtParams(String extParams) {
        this.extParams = extParams;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public Boolean getUseSSL() {
        return useSSL;
    }

    public void setUseSSL(Boolean useSSL) {
        this.useSSL = useSSL;
    }

    public String getSslCa() {
        return sslCa;
    }

    public void setSslCa(String sslCa) {
        this.sslCa = sslCa;
    }

    public String getSslCert() {
        return sslCert;
    }

    public void setSslCert(String sslCert) {
        this.sslCert = sslCert;
    }

    public String getSslKey() {
        return sslKey;
    }

    public void setSslKey(String sslKey) {
        this.sslKey = sslKey;
    }

    public String getSslKeyPassword() {
        return sslKeyPassword;
    }

    public void setSslKeyPassword(String sslKeyPassword) {
        this.sslKeyPassword = sslKeyPassword;
    }
}
