package io.tapdata.common;

import io.tapdata.common.util.FileUtil;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

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
    private Boolean hashSplit = false;
    private int maxSplit = 20;
    private int batchReadThreadSize = 4;
    private Boolean doubleActive = false;
    private Boolean oldVersionTimezone = false;
    private Boolean createAutoInc = false;
    private long autoIncStartValue = 1000000L;
    private Integer writeThreadSize = 15;
    protected String timezone = "+00:00";
    protected ZoneId zoneId;
    protected Integer zoneOffsetHour;
    protected ZoneId sysZoneId;

    protected Boolean enableFileInput = false;
    protected Long bufferCapacity = 10000000L;

    private Boolean useSSL = false;
    private String sslCa;
    private String sslCert;
    private String sslKey;
    private String sslKeyPassword;
    protected String sslRandomPath;

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
            if (EmptyKit.isBlank(timezone)) {
                timezone = "+00:00";
            }
            TimeZone timeZone = TimeZone.getTimeZone("GMT" + timezone);
            zoneId = timeZone.toZoneId();
            zoneOffsetHour = timeZone.getRawOffset() / 1000 / 60 / 60;
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
        if (EmptyKit.isNull(properties)) {
            properties = new Properties();
        }
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        if (EmptyKit.isBlank(timezone)) {
            timezone = "+00:00";
        }
        TimeZone timeZone = TimeZone.getTimeZone("GMT" + timezone);
        zoneId = timeZone.toZoneId();
        zoneOffsetHour = timeZone.getRawOffset() / 1000 / 60 / 60;
        if (useSSL && EmptyKit.isNotEmpty(map) && map.containsKey("useSSL")) {
            try {
                generateSSlFile();
            } catch (Exception e) {
                deleteSSlFile();
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

    public void generateSSlFile() throws Exception {
        throw new UnsupportedOperationException("generate ssl file failed");
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

    public Boolean getHashSplit() {
        return hashSplit;
    }

    public void setHashSplit(Boolean hashSplit) {
        this.hashSplit = hashSplit;
    }

    public int getMaxSplit() {
        return maxSplit;
    }

    public void setMaxSplit(int maxSplit) {
        this.maxSplit = maxSplit;
    }

    public int getBatchReadThreadSize() {
        return batchReadThreadSize;
    }

    public void setBatchReadThreadSize(int batchReadThreadSize) {
        this.batchReadThreadSize = batchReadThreadSize;
    }

    public Boolean getDoubleActive() {
        return doubleActive;
    }

    public void setDoubleActive(Boolean doubleActive) {
        this.doubleActive = doubleActive;
    }

    public Boolean getOldVersionTimezone() {
        return oldVersionTimezone;
    }

    public void setOldVersionTimezone(Boolean oldVersionTimezone) {
        this.oldVersionTimezone = oldVersionTimezone;
    }

    public Boolean getCreateAutoInc() {
        return createAutoInc;
    }

    public void setCreateAutoInc(Boolean createAutoInc) {
        this.createAutoInc = createAutoInc;
    }

    public long getAutoIncStartValue() {
        return autoIncStartValue;
    }

    public void setAutoIncStartValue(long autoIncStartValue) {
        this.autoIncStartValue = autoIncStartValue;
    }

    public Integer getWriteThreadSize() {
        return writeThreadSize;
    }

    public void setWriteThreadSize(Integer writeThreadSize) {
        this.writeThreadSize = writeThreadSize;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public void setZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public Integer getZoneOffsetHour() {
        return zoneOffsetHour;
    }

    public void setZoneOffsetHour(Integer zoneOffsetHour) {
        this.zoneOffsetHour = zoneOffsetHour;
    }

    public ZoneId getSysZoneId() {
        return sysZoneId;
    }

    public void setSysZoneId(ZoneId sysZoneId) {
        this.sysZoneId = sysZoneId;
    }

    public Boolean getEnableFileInput() {
        return enableFileInput;
    }

    public void setEnableFileInput(Boolean enableFileInput) {
        this.enableFileInput = enableFileInput;
    }

    public Long getBufferCapacity() {
        return bufferCapacity;
    }

    public void setBufferCapacity(Long bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
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

    public String getSslRandomPath() {
        return sslRandomPath;
    }

    public void setSslRandomPath(String sslRandomPath) {
        this.sslRandomPath = sslRandomPath;
    }
}
