package io.tapdata.connector.mrskafka.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.UUID;

public class MrsProducerConfiguration extends MrsAbstractConfiguration {
    public final static String BOOTSTRAP_SERVER = "bootstrap.servers";

    // 客户端ID
    public final static String CLIENT_ID = "client.id";


    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    public final static String SECURITY_PROTOCOL = "security.protocol";

    // 服务名
    public final static String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";

    // 域名
    public final static String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";


    public MrsProducerConfiguration(MrsKafkaConfig mrsKafkaConfig, String connectorId) {
        super(mrsKafkaConfig, connectorId);
    }

    @Override
    public Map<String, Object> build() {
        final String tid = String.format("TapData-KafkaTarget-%s-%s", connectorId, UUID.randomUUID());
        configMap.put(BOOTSTRAP_SERVER, mrsKafkaConfig.getNameSrvAddr());
        configMap.put(CLIENT_ID, tid);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        configMap.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        if (this.mrsKafkaConfig.getKafkaProducerRequestTimeout() > 0) {
            configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.mrsKafkaConfig.getKafkaProducerRequestTimeout());
        }
        if (this.mrsKafkaConfig.getKafkaRetries() > 0) {
            configMap.put(ProducerConfig.RETRIES_CONFIG, this.mrsKafkaConfig.getKafkaRetries());
        } else {
            configMap.put(ProducerConfig.RETRIES_CONFIG, 1);
        }
        if (this.mrsKafkaConfig.getKafkaBatchSize() > 0) {
            configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, this.mrsKafkaConfig.getKafkaBatchSize());
        }
        if (StringUtils.equalsAny(this.mrsKafkaConfig.getKafkaAcks(), "0", "1", "-1", "all")) {
            // ENABLE_IDEMPOTENCE_CONFIG is controlled by kafka ack setting
            boolean enableIdempotence = StringUtils.equalsAny(this.mrsKafkaConfig.getKafkaAcks(), "-1", "all");
            configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
            configMap.put(ProducerConfig.ACKS_CONFIG, this.mrsKafkaConfig.getKafkaAcks());
        }
        if (this.mrsKafkaConfig.getKafkaLingerMS() >= 0) {
            configMap.put(ProducerConfig.LINGER_MS_CONFIG, this.mrsKafkaConfig.getKafkaLingerMS());
        }
        if (this.mrsKafkaConfig.getKafkaMaxRequestSize() > 0) {
            configMap.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, this.mrsKafkaConfig.getKafkaMaxRequestSize());
        }
        if (this.mrsKafkaConfig.getKafkaBufferMemory() > 0) {
            configMap.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.mrsKafkaConfig.getKafkaBufferMemory());
        }
        if (this.mrsKafkaConfig.getKafkaMaxBlockMS() > 0) {
            configMap.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, this.mrsKafkaConfig.getKafkaMaxBlockMS());
        }
        if (this.mrsKafkaConfig.getKafkaDeliveryTimeoutMS() > 0) {
            configMap.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, this.mrsKafkaConfig.getKafkaDeliveryTimeoutMS());
        }
        if (StringUtils.equalsAny(this.mrsKafkaConfig.getKafkaCompressionType(), "gzip", "snappy", "lz4", "zstd")) {
            configMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.mrsKafkaConfig.getKafkaCompressionType());
        }
        if (this.mrsKafkaConfig.getKafkaProducerUseTransactional()) {
            configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, tid);
        }

        return super.build();
    }
}
