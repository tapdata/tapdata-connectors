package io.tapdata.connector.rds;

import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.kit.EmptyKit;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

public class AliyunRDSMySQLConfig extends MysqlConfig {
    @Override
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
            try{
                FileUtil.save(Base64.getUrlDecoder().decode(getSslCa()), sslCaPath, true);
                Runtime.getRuntime().exec("keytool -import -noprompt -file " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "ca.pem") +
                        " -keystore " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "truststore.jks") + " -storepass 123456").waitFor();
                properties.put("trustCertificateKeyStoreUrl", "file:" + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "truststore.jks"));
                properties.put("trustCertificateKeyStorePassword", "123456");
            }catch (Exception e){
                String jksPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "truststore.jks");
                FileUtil.save(Base64.getDecoder().decode(getSslCa()), jksPath, true);
                properties.put("trustCertificateKeyStoreUrl", "file:" + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "truststore.jks"));
                properties.put("trustCertificateKeyStorePassword", "apsaradb");
            }
        }
        //如果客户端证书有内容，表示需要验证客户端证书，导入keystore.jks
        if (EmptyKit.isNotBlank(getSslCert()) && EmptyKit.isNotBlank(getSslKey())) {
            String sslCertPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslCert()), sslCertPath, true);
            String sslKeyPath = FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem");
            FileUtil.save(Base64.getUrlDecoder().decode(getSslKey()), sslKeyPath, true);
            //openssl低版本不需要加-legacy
            Runtime.getRuntime().exec("openssl pkcs12 -export -in " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem") +
                    " -inkey " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem") +
                    " -name datasource-client -passout pass:123456 -out " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client-keystore.p12")).waitFor();
            //openssl高版本需要加-legacy
            Runtime.getRuntime().exec("openssl pkcs12 -legacy -export -in " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "cert.pem") +
                    " -inkey " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "key.pem") +
                    " -name datasource-client -passout pass:123456 -out " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client-keystore.p12")).waitFor();
            Runtime.getRuntime().exec("keytool -importkeystore -srckeystore " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "client-keystore.p12") +
                    " -srcstoretype pkcs12 -srcstorepass 123456 -destkeystore " + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "keystore.jks") + " -deststoretype JKS -deststorepass 123456").waitFor();
            properties.put("clientCertificateKeyStoreUrl", "file:" + FileUtil.paths(FileUtil.storeDir(".ssl"), sslRandomPath, "keystore.jks"));
            properties.put("clientCertificateKeyStorePassword", "123456");
        }
        if (EmptyKit.isNotBlank(getSslKeyPassword())) {
            properties.put("clientKeyPassword", getSslKeyPassword());
        }
    }
}
