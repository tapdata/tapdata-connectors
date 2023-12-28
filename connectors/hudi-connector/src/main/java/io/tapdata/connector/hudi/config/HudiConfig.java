package io.tapdata.connector.hudi.config;

import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.connector.hudi.util.KerberosUtil;
import io.tapdata.connector.hudi.util.Krb5Util;
import io.tapdata.kit.EmptyKit;

import java.util.Map;

public class HudiConfig extends HiveConfig {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";


    public HudiConfig(String connectorId) {
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
        init();
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?") && !this.getExtParams().startsWith(":")) {
            this.setExtParams("" + this.getExtParams());
        }
        StringBuilder strBuilder = new StringBuilder(String.format(this.getDatabaseUrlPattern(), this.getNameSrvAddr(), this.getDatabase(), this.getExtParams()));
        if (krb5) {
            String keytabPath = Krb5Util.keytabPath(krb5Path);
            keytabPath = keytabPath.replaceAll("\\\\","/");
            strBuilder.append(";principal=")
                    .append(principal)
                    .append(";user.principal=")
                    .append(getUser())
                    .append(";user.keytab=")
                    .append(keytabPath)
                    .append(";");
        }
        return strBuilder.toString();

    }

    @Override
    public HudiConfig load(Map<String, Object> map) {
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        if (krb5) {
            krb5Path = Krb5Util.saveByCatalog("connections-" + connectorId, krb5Keytab, krb5Conf, true);
        }
        return this;
    }

    public  void init(){
        if (krb5) {
            String confPath=  Krb5Util.confPath(this.getKrb5Path());
            confPath = confPath.replaceAll("\\\\","/");
            System.setProperty("java.security.krb5.conf", confPath);
            String zkPrincipal = "zookeeper/" + getUserRealm(this.getKrb5Conf());
            System.setProperty("zookeeper.server.principal", zkPrincipal);
        }
        if (this.getSsl()) {
            System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
            System.setProperty("zookeeper.client.secure", "true");
        }
    }

    public static String getUserRealm(String krb5Conf) {
        String serverRealm = System.getProperty("SERVER_REALM");
        String authHostName;
        if (serverRealm != null && !serverRealm.equals("")) {
            authHostName = "hadoop." + serverRealm.toLowerCase();
        } else {
            serverRealm = KerberosUtil.getKrb5DomainRealm();
            if (serverRealm != null && !serverRealm.equals("")) {
                authHostName = "hadoop." + serverRealm.toLowerCase();
            } else {
                authHostName = "hadoop";
            }
        }
        return authHostName;
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

    public String authFilePath(String filePath) {
        System.out.println(filePath);
        return "D:\\Gavin\\kit\\IDEA\\hudi-demo\\java-client\\src\\main\\resources\\" + filePath;
    }
}
