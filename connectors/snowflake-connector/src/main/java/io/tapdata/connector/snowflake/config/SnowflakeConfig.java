package io.tapdata.connector.snowflake.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.util.FileUtil;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/**
 * Snowflake Configuration
 *
 * @author Jarad
 * @date 2026/03/24
 */
public class SnowflakeConfig extends CommonDbConfig {

    private String account;
    private String warehouse;
    private String role;
    private Boolean useKeyPair = false;
    private String privateKey;
    private String privateKeyPassword;
    private String privateKeyRandomPath;

    public SnowflakeConfig() {
        setDbType("snowflake");
        setJdbcDriver("net.snowflake.client.jdbc.SnowflakeDriver");
        setEscapeChar('"');
    }

    @Override
    public String getConnectionString() {
        return "jdbc:snowflake://" + account + ".snowflakecomputing.com:443";
    }

    public String getDatabaseUrl() {
        return "jdbc:snowflake://" + account + ".snowflakecomputing.com:443" +
                "?warehouse=" + warehouse +
                "&db=" + getDatabase();
    }

    @Override
    public Properties getProperties() {
        Properties props = super.getProperties();
        if (Boolean.TRUE.equals(useKeyPair) && EmptyKit.isNotBlank(privateKey)) {
            if (null == props) {
                props = new Properties();
                setProperties(props);
            }
            props.put("authenticator", "SNOWFLAKE_JWT");
            props.put("private_key_file", ensurePrivateKeyFile());
            if (EmptyKit.isNotBlank(privateKeyPassword)) {
                props.put("private_key_file_pwd", privateKeyPassword);
            }
        }
        return props;
    }

    private synchronized String ensurePrivateKeyFile() {
        if (EmptyKit.isBlank(privateKeyRandomPath)) {
            privateKeyRandomPath = UUID.randomUUID().toString().replace("-", "");
        }
        String keyPath = FileUtil.paths(FileUtil.storeDir(".snowflake"), privateKeyRandomPath, "rsa_key.pem");
        if (!new File(keyPath).exists()) {
            try {
                FileUtil.save(privateKey.getBytes(), keyPath, true);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write Snowflake private key file", e);
            }
        }
        return keyPath;
    }

    public void deletePrivateKeyFile() {
        if (EmptyKit.isNotBlank(privateKeyRandomPath)) {
            File cacheDir = new File(FileUtil.paths(FileUtil.storeDir(".snowflake"), privateKeyRandomPath));
            if (cacheDir.exists()) {
                ErrorKit.ignoreAnyError(() -> FileUtils.deleteDirectory(cacheDir));
            }
        }
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Boolean getUseKeyPair() {
        return useKeyPair;
    }

    public void setUseKeyPair(Boolean useKeyPair) {
        this.useKeyPair = useKeyPair;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public String getPrivateKeyPassword() {
        return privateKeyPassword;
    }

    public void setPrivateKeyPassword(String privateKeyPassword) {
        this.privateKeyPassword = privateKeyPassword;
    }
}

