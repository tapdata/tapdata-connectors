package io.tapdata.connector.hudi.config;

import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.connector.hudi.util.FileUtil;
import io.tapdata.connector.hudi.util.KerberosUtil;
import io.tapdata.connector.hudi.util.Krb5Util;
import io.tapdata.connector.hudi.util.SiteXMLUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.EmptyKit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AnnotatedSecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Map;

public class HudiConfig extends HiveConfig implements AutoCloseable {
    public static final Object AUTH_LOCK = new Object();
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private String hadoopCoreSiteXML;
    private String hdfsSiteXML;
    private String hiveSiteXML;
    Log log;

    public HudiConfig(String connectorId) {
        super();
        this.connectorId = connectorId;
    }

    public HudiConfig log(Log log) {
        this.log = log;
        return this;
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
        final String catalog = "hudi" + connectorId;
        if (krb5) {
            krb5Path = Krb5Util.saveByCatalog(catalog, krb5Keytab, krb5Conf, true);
        }
        //save core-site.xml, hdfs-site.xml, hive-site.xml
        this.xmlPath = SiteXMLUtil.saveSiteXMLS(catalog, hadoopCoreSiteXML, hdfsSiteXML, hiveSiteXML);
        init();
        return this;
    }

    public void init(){
        if (krb5) {
            String confPath = FileUtil.paths(this.getKrb5Path(), Krb5Util.KRB5_NAME);
            confPath = confPath.replaceAll("\\\\","/");
            System.setProperty("java.security.krb5.conf", confPath);
            //authenticate(new Configuration());
            String zkPrincipal = "zookeeper/" + getUserRealm(this.getKrb5Conf());
            System.setProperty("zookeeper.server.principal", zkPrincipal);
        }
        if (this.getSsl()) {
            System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
            System.setProperty("zookeeper.client.secure", "true");
        }
    }

    public HudiConfig authenticate(Configuration conf1) {
        if (krb5) {
            String localKeytabPath = FileUtil.paths(this.getKrb5Path(), Krb5Util.USER_KEY_TAB_NAME);
            String confPath = FileUtil.paths(this.getKrb5Path(), Krb5Util.KRB5_NAME);
            String krb5Path = confPath.replaceAll("\\\\","/");
            final String principal = getUser() + "@" + KerberosUtil.DEFAULT_REALM;
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader loader = AnnotatedSecurityInfo.class.getClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(loader);
                synchronized (AUTH_LOCK) {
                    Configuration.reloadExistingConfigurations();
                    System.clearProperty("java.security.krb5.conf");
                    System.setProperty("java.security.krb5.conf", krb5Path);
                    Configuration conf = new Configuration();
                    conf.set("hadoop.security.authentication", "kerberos");
                    UserGroupInformation.setConfiguration(conf);
                    UserGroupInformation.loginUserFromKeytab(principal, localKeytabPath);
                    SecurityUtil.setSecurityInfoProviders(new AnnotatedSecurityInfo());
                }
                if (null != log) {
                    log.info("Safety certification passed");
                }
            } catch (Exception e) {
                throw new CoreException("Fail to get certification, message: {}", e.getMessage(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(classLoader);
            }
        }
        return this;
    }

    public static String getUserRealm(String krb5Conf) {
        String serverRealm = System.getProperty("SERVER_REALM");
        String authHostName;
        if (serverRealm != null && !serverRealm.isEmpty()) {
            authHostName = "hadoop." + serverRealm.toLowerCase();
        } else {
            serverRealm = KerberosUtil.getKrb5DomainRealm();
            if (serverRealm != null && !serverRealm.isEmpty()) {
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
    private String xmlPath;
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

    public String getHadoopCoreSiteXML() {
        return hadoopCoreSiteXML;
    }

    public void setHadoopCoreSiteXML(String hadoopCoreSiteXML) {
        this.hadoopCoreSiteXML = hadoopCoreSiteXML;
    }

    public String getHdfsSiteXML() {
        return hdfsSiteXML;
    }

    public void setHdfsSiteXML(String hdfsSiteXML) {
        this.hdfsSiteXML = hdfsSiteXML;
    }

    public String getHiveSiteXML() {
        return hiveSiteXML;
    }

    public void setHiveSiteXML(String hiveSiteXML) {
        this.hiveSiteXML = hiveSiteXML;
    }

    public String authFilePath(String filePath) {
        return FileUtil.paths(xmlPath, filePath);
    }

    @Override
    public void close() throws Exception {
        //FileUtil.release( "hudi" + connectorId, log);
    }
}
