package io.tapdata.connector.hudi.util;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Kerberos 工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/17 下午5:04 Create
 */
public class Krb5Util {
    private static final Logger logger = LoggerFactory.getLogger(KerberosUtil.class);

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    public static final String KRB5_NAME = "krb5.conf";

    public static final String USER_KEY_TAB_NAME = "user.keytab";
    public static final String TAG = Krb5Util.class.getSimpleName();

    private static String decodeConf(String base64Conf) {
        byte[] bytes = Base64.getUrlDecoder().decode(base64Conf);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static Map<String, Map<String, String>> getRealms(String krb5Conf) {
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

    /**
     * 获取配置
     *
     * @param krb5Path      授权路径
     * @param krb5Principal 主体
     * @return 配置
     */
    public static String saslJaasConfig(String krb5Path, String krb5Principal) {
        return "com.sun.security.auth.module.Krb5LoginModule required\n" +
                "    useKeyTab=true\n" +
                "    storeKey=true\n" +
                "    useTicketCache=true\n" +
                "    keyTab=\"" + keytabPath(krb5Path) + "\"\n" +
                "    principal=\"" + krb5Principal + "\";";
    }

    /**
     * 获取密钥路径
     *
     * @param dir 配置目录
     * @return 密钥路径
     */
    public static String keytabPath(String dir) {
        return FileUtil.paths(dir, USER_KEY_TAB_NAME);
    }

    /**
     * 检查配置域
     *
     * @param conf 配置
     * @throws UnknownHostException 异常
     */
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

    /**
     * 检查配置域
     *
     * @param base64Conf base64配置
     * @throws UnknownHostException 异常
     */
    public static void checkKDCDomainsBase64(String base64Conf) throws UnknownHostException {
        String conf = decodeConf(base64Conf);
        checkKDCDomains(conf);
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
        String dir = FileUtil.paths(FileUtil.storeDir(catalog), "krb5");
        if (new File(dir).exists()) return dir;
        try {
            FileUtil.saveBase64File(dir, USER_KEY_TAB_NAME, keytab, deleteExists);
        } catch (Exception e) {
            throw new CoreException("Save kerberos keytab failed: {}/user.keytab", dir, e);
        }
        try {
            FileUtil.saveBase64File(dir, KRB5_NAME, conf, deleteExists);
        } catch (Exception e) {
            throw new CoreException("Save kerberos conf failed: {}/krb5.conf", dir, e);
        }
        return dir;
    }

    private static void setKrb5Config(String krb5ConfFilePath) throws IOException {
        String paths = FileUtil.paths(krb5ConfFilePath, KRB5_NAME);
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, paths);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFilePath)) {
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + paths );
        }
    }

    /**
     * 更新 Kafka 配置
     *
     * @param serviceName 服务名
     * @param principal   主体
     * @param krb5Path    授权配置目录
     * @param krb5Conf    授权配置
     * @param conf        kafka配置
     */
    public static void updateKafkaConf(String serviceName, String principal, String krb5Path, String krb5Conf, Map<String, Object> conf) {
        try {
            String krb5ConfPath = FileUtil.paths(krb5Path, KRB5_NAME);
            String saslJaasConfig = saslJaasConfig(krb5Path, principal);

            conf.put("security.protocol", "SASL_PLAINTEXT");
            conf.put("sasl.mechanism", "GSSAPI");
            conf.put("sasl.kerberos.service.name", serviceName);
            conf.put("sasl.jaas.config", saslJaasConfig);
            System.setProperty("java.security.krb5.conf", krb5ConfPath);

            String realm = null;
            if (null != principal) {
                String[] arr = principal.split("@");
                if (arr.length == 2) realm = arr[1].trim();
            }
            if (null == realm || realm.isEmpty()) {
                TapLogger.warn(TAG, "Parse krb5 realm failed: " + principal);
                return;
            }

            krb5Conf = decodeConf(krb5Conf);
            Map<String, Map<String, String>> realms = getRealms(krb5Conf);
            Map<String, String> currentRealms = realms.get(realm);
            if (null != currentRealms && currentRealms.containsKey("kdc")) {
                System.setProperty("java.security.krb5.realm", realm);
                System.setProperty("java.security.krb5.kdc", currentRealms.get("kdc"));

                Config.refresh();
            } else {
                TapLogger.warn(TAG, "Not found kdc in realm '{}' >> {}", realm, krb5Conf);
            }
        } catch (KrbException e) {
            throw new RuntimeException("Refresh krb5 config failed", e);
        }
    }
}
