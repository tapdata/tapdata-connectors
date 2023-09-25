package io.tapdata.connector.adb;

import io.tapdata.connector.mysql.config.MysqlConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class AliyunMysqlConfig extends MysqlConfig {

    private static final Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
        put("rewriteBatchedStatements", "true");
        put("useSSL", "false");
        put("zeroDateTimeBehavior", "convertToNull");
        put("allowPublicKeyRetrieval", "true");
        put("useTimezone", "false");
        put("tinyInt1isBit", "false");
        put("autoReconnect", "true");
    }};

    @Override
    public String getDatabaseUrl() {
        String additionalString = getExtParams();
        additionalString = null == additionalString ? "" : additionalString.trim();
        if (additionalString.startsWith("?")) {
            additionalString = additionalString.substring(1);
        }

        Map<String, String> properties = new HashMap<>();
        StringBuilder sbURL = new StringBuilder("jdbc:").append(getDbType()).append("://").append(getHost()).append(":").append(getPort()).append("/").append(getDatabase());

        if (StringUtils.isNotBlank(additionalString)) {
            String[] additionalStringSplit = additionalString.split("&");
            for (String s : additionalStringSplit) {
                String[] split = s.split("=");
                if (split.length == 2) {
                    properties.put(split[0], split[1]);
                }
            }
        }
        for (String defaultKey : DEFAULT_PROPERTIES.keySet()) {
            if (properties.containsKey(defaultKey)) {
                continue;
            }
            properties.put(defaultKey, DEFAULT_PROPERTIES.get(defaultKey));
        }

        if (StringUtils.isNotBlank(timezone)) {
            try {
                timezone = "GMT" + timezone;
                String serverTimezone = timezone.replace("+", "%2B").replace(":00", "");
                properties.put("serverTimezone", serverTimezone);
            } catch (Exception ignored) {
            }
        }
        StringBuilder propertiesString = new StringBuilder();
        properties.forEach((k, v) -> propertiesString.append("&").append(k).append("=").append(v));

        if (propertiesString.length() > 0) {
            additionalString = StringUtils.removeStart(propertiesString.toString(), "&");
            sbURL.append("?").append(additionalString);
        }

        return sbURL.toString();
    }
}
