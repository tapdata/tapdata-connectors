package io.tapdata.connector.mrskafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.UUID;

import static io.tapdata.connector.mrskafka.config.MrsProducerConfiguration.*;

public class MrsConsumerConfiguration extends MrsAbstractConfiguration {
    private final boolean isEarliest;

    public MrsConsumerConfiguration(MrsKafkaConfig mrsKafkaConfig, String connectorId, boolean isEarliest) {
        super(mrsKafkaConfig, connectorId);
        this.isEarliest = isEarliest;
    }

    @Override
    public Map<String, Object> build() {
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.mrsKafkaConfig.getNameSrvAddr());
        configMap.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("TapData-KafkaSource-%s-%s", connectorId, UUID.randomUUID()));
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("TapData-KafkaSource-%s", this.connectorId));
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.isEarliest ? "earliest" : "latest");
        configMap.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        if (this.mrsKafkaConfig.getKafkaConsumerUseTransactional()) {
            configMap.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        if (this.mrsKafkaConfig.getKafkaConsumerRequestTimeout() > 0) {
            configMap.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.mrsKafkaConfig.getKafkaConsumerRequestTimeout());
        }
        if (this.mrsKafkaConfig.getKafkaMaxPollRecords() > 0) {
            configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.mrsKafkaConfig.getKafkaMaxPollRecords());
        }
        if (this.mrsKafkaConfig.getKafkaMaxFetchBytes() > 0) {
            configMap.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, this.mrsKafkaConfig.getKafkaMaxFetchBytes());
        }
        if (this.mrsKafkaConfig.getKafkaMaxFetchWaitMS() > 0) {
            configMap.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, this.mrsKafkaConfig.getKafkaMaxFetchWaitMS());
        }
        return super.build();
    }

    public boolean isEarliest() {
        return isEarliest;
    }
}
