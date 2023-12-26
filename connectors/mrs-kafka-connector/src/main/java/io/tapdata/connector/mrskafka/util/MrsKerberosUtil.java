package io.tapdata.connector.mrskafka.util;

import io.tapdata.connector.mrskafka.config.MrsKafkaConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class MrsKerberosUtil {
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";
    private static final String JAAS_POSTFIX = ".jaas.conf";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    public static final String JAVA_SECURITY_LOGIN_CONF = "java.security.auth.login.config";

    public static void updateKafkaConf(String krb5Path, String huaweiZookeeperPrincipal) {
        String krb5ConfPath = confPath(krb5Path);
        setKrb5Config(krb5ConfPath);
        setZookeeperServerPrincipal(huaweiZookeeperPrincipal);
    }

    public static void setKrb5Config(String krb5ConfFile) {
        krb5ConfFile = krb5ConfFile.replace("\\", "\\\\");
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5ConfFile);
    }

    public static void setZookeeperServerPrincipal(String zkServerPrincipal) {
        System.setProperty(ZOOKEEPER_AUTH_PRINCIPAL, zkServerPrincipal);
        String ret = System.getProperty(ZOOKEEPER_AUTH_PRINCIPAL);

    }

    public static void setJaasFile(String catalog, String principal, String keytabPath) {
        String jaasPath = paths(storeDir(), catalog);
        String jassFileName = "jaasscontent" + JAAS_POSTFIX;
        String jassFilePath = paths(jaasPath, jassFileName);
        jassFilePath = jassFilePath.replace("\\", "\\\\");
        keytabPath = keytabPath.replace("\\", "\\\\");
        deleteJaasFile(jassFilePath);
        writeJaasFile(jassFilePath, principal, keytabPath);
        System.setProperty(JAVA_SECURITY_LOGIN_CONF, jassFilePath);
    }

    private static void deleteJaasFile(String jaasPath) {
        File jaasFile = new File(jaasPath);
        if (jaasFile.exists() && !jaasFile.delete()) {
            throw new RuntimeException("Delete JaasFile Faild");
        }
    }

    private static void writeJaasFile(String jassFilePath, String principal, String keytabPath) {
        FileWriter writer = null;
        try {
            writer = new FileWriter(new File(jassFilePath));
            writer.write(getJaasConfContext(principal, keytabPath));
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String getJaasConfContext(String principal, String keytabPath) {
        Module[] allModule = Module.values();
        StringBuilder builder = new StringBuilder();
        for (Module modlue : allModule) {
            builder.append(getModuleContext(principal, keytabPath, modlue));
        }
        return builder.toString();
    }

    private static String getModuleContext(String userPrincipal, String keyTabPath, Module module) {
        StringBuilder builder = new StringBuilder();
        builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
        builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
        builder.append("useKeyTab=true").append(LINE_SEPARATOR);
        builder.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
        builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
        builder.append("useTicketCache=false").append(LINE_SEPARATOR);
        builder.append("storeKey=true").append(LINE_SEPARATOR);
        builder.append("debug=true;").append(LINE_SEPARATOR);
        builder.append("};").append(LINE_SEPARATOR);
        return builder.toString();
    }

    private static String storeDir() {
        String dir = System.getenv("TAPDATA_WORK_DIR");
        if (null == dir) {
            dir = ".";
        }
        return paths(dir, "krb5");
    }


    private static String paths(String... paths) {
        return String.join(File.separator, paths);
    }

    public enum Module {
        KAFKA("KafkaClient"), ZOOKEEPER("Client");

        private String name;

        private Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * 根据类别保存
     *
     * @param catalog      类别
     * @param keytab       密钥
     * @param conf         配置
     * @param deleteExists 存在删除
     * @return 配置路径
     */
    public static String saveByCatalog(String catalog, String keytab, String conf, boolean deleteExists) {
        byte[] bytes;
        String savePath = null;
        String dir = paths(storeDir(), catalog);
        try {
            savePath = keytabPath(dir);
            savePath = savePath.replace("\\", "\\\\");
            bytes = Base64.getDecoder().decode(keytab);
            save(bytes, savePath, deleteExists);
        } catch (Exception e) {
            throw new RuntimeException("Save kerberos keytab failed: " + savePath, e);
        }
        try {
            savePath = confPath(dir);
            savePath = savePath.replace("\\", "\\\\");
            bytes = Base64.getDecoder().decode(conf);
            save(bytes, savePath, deleteExists);
        } catch (Exception e) {
            throw new RuntimeException("Save kerberos conf failed: " + savePath, e);
        }
        return dir;
    }

    /**
     * 保存文件
     *
     * @param data         数据
     * @param savePath     保存路径
     * @param deleteExists 是否存在删除
     * @throws IOException 异常
     */
    private static void save(byte[] data, String savePath, boolean deleteExists) throws IOException {
        File file = new File(savePath);
        if (file.exists()) {
            if (!deleteExists) {
                throw new RuntimeException("Save config is exists: " + savePath);
            }
            file.delete();
        } else {
            File dir = file.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
    }

    /**
     * 获取密钥路径
     *
     * @param dir 配置目录
     * @return 密钥路径
     */
    public static String keytabPath(String dir) {
        return paths(dir, "krb5.keytab");
    }

    /**
     * 获取配置路径
     *
     * @param dir 配置目录
     * @return 配置路径
     */
    public static String confPath(String dir) {
        return paths(dir, "krb5.conf");
    }

    public static void checkKDCDomainsBase64(String base64Conf) throws UnknownHostException {
        String conf = decodeConf(base64Conf);
        checkKDCDomains(conf);
    }

    public static void checkKDCDomains(String conf) throws UnknownHostException {
        Map<String, Map<String, String>> realms = getRealms(conf);
        for (Map<String, String> realm : realms.values()) {
            for (Map.Entry<String, String> en : realm.entrySet()) {
                if (StringUtils.containsAny(en.getKey(), "kdc", "master_kdc", "admin_server", "default_domain")
                        && null != en.getValue()) {
                    String s = en.getValue().split(":")[0].trim();
                    if (!s.isEmpty()) {
                        InetAddress.getAllByName(s);
                    }
                }
            }
        }
    }

    private static String decodeConf(String base64Conf) {
        byte[] bytes = Base64.getUrlDecoder().decode(base64Conf);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Map<String, Map<String, String>> getRealms(String krb5Conf) {
        try {
            String[] kv;
            boolean inRealms = false, begin = true;
            Map<String, String> kvMap = null;
            Map<String, Map<String, String>> map = new HashMap<>();
            for (String line : krb5Conf.split("\n")) {
                if (line.startsWith("#")) continue;
                line = line.replaceAll("#.*", "").trim();
                if (line.isEmpty()) continue;

                if (!inRealms) {
                    if (Pattern.matches("^[\\[]*\\[realms].*", line)) {
                        inRealms = true;
                    }
                    continue;
                } else if (line.startsWith("[")) {
                    break;
                }

                if (begin) {
                    kvMap = new HashMap<>();
                    map.put(line.replaceAll("([^\\s]+).*", "$1"), kvMap);
                    begin = false;
                    continue;
                } else if (line.contains("}")) {
                    begin = true;
                    continue;
                }
                kv = line.split("=");
                if (kv.length != 2) continue;
                kvMap.put(kv[0].trim(), kv[1].trim());
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("Parse [realms] failed: " + e.getMessage(), e);
        }
    }
}
