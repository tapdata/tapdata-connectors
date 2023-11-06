package io.tapdata.connector.hive3;

import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.kit.EmptyKit;

import java.util.Map;

public class Hive3Config extends HiveConfig {


    public Hive3Config(String connectorId) {
        super();
        this.connectorId = connectorId;
    }

    public String getDatabaseUrlPattern() {
        return "jdbc:" + getDbType() + "://%s/%s%s";
    }

    public String getConnectionString() {
        String connectionString = nameSrvAddr + "/" + getDatabase();
        if (EmptyKit.isNotBlank(getSchema())) {
            connectionString += "/" + getSchema();
        }
        return connectionString;
    }

    public String getDatabaseUrl()  {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?") && !this.getExtParams().startsWith(":")) {
            this.setExtParams("" + this.getExtParams());
        }
        StringBuilder strBuilder = new StringBuilder(String.format(this.getDatabaseUrlPattern(), this.getHost()+":"+this.getPort(), this.getDatabase(), this.getExtParams()));
        return strBuilder.toString();

    }

    @Override
    public Hive3Config load(Map<String, Object> map) {
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        return this;
    }



    private final String connectorId;
    private String nameSrvAddr;
    private Boolean krb5;
    private String krb5Path;
    private String krb5Keytab;
    private String krb5Conf;
    private Boolean ssl;
    private String principal;
    private String hdfsAddr;

    public String getNameSrvAddr() {
        return nameSrvAddr;
    }

    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }

    public Boolean getKrb5() {
        return krb5;
    }

    public void setKrb5(Boolean krb5) {
        this.krb5 = krb5;
    }

    public String getKrb5Path() {
        return krb5Path;
    }

    public void setKrb5Path(String krb5Path) {
        this.krb5Path = krb5Path;
    }

    public String getKrb5Keytab() {
        return krb5Keytab;
    }

    public void setKrb5Keytab(String krb5Keytab) {
        this.krb5Keytab = krb5Keytab;
    }

    public String getKrb5Conf() {
        return krb5Conf;
    }

    public void setKrb5Conf(String krb5Conf) {
        this.krb5Conf = krb5Conf;
    }

    public Boolean getSsl() {
        return ssl;
    }

    public void setSsl(Boolean ssl) {
        this.ssl = ssl;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getHdfsAddr() {
        return hdfsAddr;
    }

    public void setHdfsAddr(String hdfsAddr) {
        this.hdfsAddr = hdfsAddr;
    }
}
