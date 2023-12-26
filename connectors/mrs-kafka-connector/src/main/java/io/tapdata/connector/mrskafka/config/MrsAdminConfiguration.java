package io.tapdata.connector.mrskafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MrsAdminConfiguration extends MrsAbstractConfiguration {
    public MrsAdminConfiguration(MrsKafkaConfig mrsKafkaConfig, String connectorId) {
        super(mrsKafkaConfig, connectorId);
    }

    @Override
    public Map<String, Object> build() {
        configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.mrsKafkaConfig.getNameSrvAddr());
        configMap.put(AdminClientConfig.CLIENT_ID_CONFIG, String.format("TapData-KafkaAdmin-%s-%s", connectorId, UUID.randomUUID()));
        configMap.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(10L));
        return super.build();
    }
}
