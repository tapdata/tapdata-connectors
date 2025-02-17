package io.tapdata.kafka.config;

import io.tapdata.connector.IConfigWithContext;

/**
 * 安全配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/12 19:23 Create
 */
public interface IConnectionSecurity extends IConfigWithContext {
    String KEY_USE_SASL = "useSasl";
    String KEY_SASL_MECHANISM = "saslMechanism";
    String KEY_SASL_USERNAME = "saslUsername";
    String KEY_SASL_PASSWORD = "saslPassword";
    String KEY_USE_SSL = "useSsl";

    enum Protocol {
        PLAINTEXT, // 没有任何 authentication
        SSL, // 一般用于 client 和 server 之间是不安全网络：支持可选的 SSL client authentication，同时也支持 encryption
        SASL_PLAINTEXT, // 一般用于内部网络：在 PLAINTEXT 的基础上加入了 client 和 server 的 authentication，但是它不支持 encryption
        SASL_SSL, // 安全等级最高模式：支持 client 和 server 的 authentication，又支持 encryption
        ;

        public static Protocol fromString(String name) {
            for (Protocol v : Protocol.values()) {
                if (v.name().equals(name)) return v;
            }
            return PLAINTEXT;
        }
    }

    enum SaslMechanism {
        PLAIN("PLAIN"),
        SCRAM_SHA_256("SCRAM-SHA-256"),
        SCRAM_SHA_512("SCRAM-SHA-512"),
        GSSAPI("GSSAPI"),
        ;
        private final String value;

        SaslMechanism(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static SaslMechanism fromString(String value) {
            for (SaslMechanism v : SaslMechanism.values()) {
                if (v.getValue().equals(value)) return v;
            }
            return PLAIN;
        }
    }

    default Protocol getSecurityProtocol() {
        if (useSasl()) {
            if (useSsl()) {
                return Protocol.SASL_SSL;
            }
            return Protocol.SASL_PLAINTEXT;
        } else if (useSsl()) {
            return Protocol.SSL;
        }
        return Protocol.PLAINTEXT;
    }

    default boolean useSasl() {
        return connectionConfigGet(KEY_USE_SASL, false);
    }

    default String getSaslMechanism() {
        return connectionConfigGet(KEY_SASL_MECHANISM, "PLAIN");
    }


    default String getSaslUsername() {
        return connectionConfigGet(KEY_SASL_USERNAME, "");
    }

    default String getSaslPassword() {
        return connectionConfigGet(KEY_SASL_PASSWORD, "");
    }

    default boolean useSsl() {
        return connectionConfigGet(KEY_USE_SSL, false);
    }

}
