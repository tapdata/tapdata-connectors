package io.tapdata.kafka.config;

import io.tapdata.connector.IConfigWithContext;

import java.util.Optional;

/**
 * Kafka ACL 配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/6 10:13 Create
 */
public interface IConnectionACL extends IConfigWithContext {
    String KEY_ACL_ENABLED = "aclEnabled";

    default boolean getConnectionAclEnabled() {
        return Optional.ofNullable(connectionConfig())
            .map(c -> c.getString(KEY_ACL_ENABLED))
            .map(Boolean::parseBoolean)
            .orElse(false);
    }

}
