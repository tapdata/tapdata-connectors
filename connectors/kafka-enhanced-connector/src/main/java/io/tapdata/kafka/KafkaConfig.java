package io.tapdata.kafka;

import io.tapdata.connector.config.BasicConfig;
import io.tapdata.connector.config.ConnectionClusterURI;
import io.tapdata.connector.config.ConnectionDatasourceInstanceInfo;
import io.tapdata.connector.config.ConnectionExtParams;
import io.tapdata.kafka.config.IConnectionACL;
import io.tapdata.kafka.config.IConnectionSecurity;
import io.tapdata.kafka.constants.KafkaAcksType;
import io.tapdata.kafka.constants.KafkaConcurrentReadMode;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.constants.KafkaSerialization;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/27 14:36 Create
 */
public class KafkaConfig extends BasicConfig implements
        IConnectionSecurity,
        ConnectionClusterURI,
        ConnectionDatasourceInstanceInfo,
        IConnectionACL,
        ConnectionExtParams {

    public KafkaConfig(TapConnectionContext context) {
        super(context);
    }

    // ---------- 连接配置 ----------

    public String getConnectionClientId(String type) {
        String clientId = connectionConfigGet("clientId", null);
        if (null == clientId || clientId.isEmpty()) {
            String firstConnectorId = getStateMapFirstConnectorId();
            if (null == firstConnectorId) {
                firstConnectorId = tapConnectionContext().getId();
            }
            String uuid = UUID.randomUUID().toString().replace("-", "");
            clientId = String.format("Tap%s-%s-%s", type, firstConnectorId, uuid);
        }
        return clientId;
    }

    public KafkaSchemaMode getConnectionSchemaMode() {
        String schemaMode = connectionConfigGet("schemaMode", KafkaSchemaMode.STANDARD.name());
        return KafkaSchemaMode.fromString(schemaMode);
    }

    public KafkaSchemaMode getNodeSchemaMode() {
        String schemaMode = nodeConfigGet("schemaMode", null);
        return StringUtils.isEmpty(schemaMode) ? null : KafkaSchemaMode.fromString(schemaMode);
    }

    public KafkaSerialization getConnectionKeySerialization() {
        String keySerializer = connectionConfigGet("keySerialization", KafkaSerialization.BINARY.getType());
        return KafkaSerialization.fromString(keySerializer);
    }

    public KafkaSerialization getConnectionValueSerialization() {
        String valueSerializer = connectionConfigGet("valueSerialization", KafkaSerialization.BINARY.getType());
        return KafkaSerialization.fromString(valueSerializer);
    }

    public KafkaAcksType getConnectionAcksType() {
        String acksTypeStr = connectionConfigGet("acksType", KafkaAcksType.WRITE_MOST_ISR.getValue());
        return KafkaAcksType.fromValue(acksTypeStr);
    }

    public String getConnectionCompressionType() {
        String compressionType = connectionConfigGet("compressionType", "lz4").toLowerCase();
        switch (compressionType) {
            case "gzip":
            case "lz4":
            case "zstd":
            case "snappy":
                return compressionType;
            default:
                return null; // disable
        }
    }

    // ---------- 节点配置 ----------

    public int getNodeReadTimeout() {
        return nodeConfigGet("readTimeout", 2000);
    }

    public int getNodeBatchMaxDelay() {
        return nodeConfigGet("batchMaxDelay", 2000);
    }

    public KafkaConcurrentReadMode getNodeConcurrentReadMode() {
//        String concurrentReadMode = nodeConfigGet("concurrentReadMode", KafkaConcurrentReadMode.SINGLE.name());
//        return KafkaConcurrentReadMode.valueOf(concurrentReadMode);
        return KafkaConcurrentReadMode.PARTITIONS; // 强制按分区并发读，其它读模式后续按需要扩展
    }

    public String getNodeConcurrentGroupId(String type) {
        String suffix = UUID.randomUUID().toString();
        return nodeConfigGet("concurrentGroupId", String.format("Tap%s-%s", type, suffix));
    }

    public int getNodeMaxConcurrentSize() {
        Integer maxConcurrentSize = nodeConfigGet("maxConcurrentSize", 1);
        return maxConcurrentSize > 0 ? maxConcurrentSize : 1;
    }

    public short getNodeReplicasSize() {
        Integer replicasSize = nodeConfigGet("replicasSize", 1);
        return replicasSize > 0 ? replicasSize.shortValue() : 1;
    }

    public int getNodePartitionSize() {
        Integer partitionSize = nodeConfigGet("partitionSize", 3);
        return partitionSize > 0 ? partitionSize : 1;
    }

    public boolean getConnectionSchemaRegister() {
        return connectionConfigGet("schemaRegister", Boolean.FALSE);
    }

    public String getConnectionSchemaRegisterUrl() {
        return connectionConfigGet("schemaRegisterUrl", "");
    }

    public String getConnectionRegistrySchemaType() {
        return connectionConfigGet("registrySchemaType", "JSON");
    }

    public String getConnectionAuthCredentialsSource() {
        return connectionConfigGet("authCredentialsSource", "");
    }

    public String getConnectionAuthUserName() {
        return connectionConfigGet("authUserName", "");
    }

    public String getConnectionAuthPassword() {
        return connectionConfigGet("authPassword", "");
    }

    public boolean getConnectionBasicAuth() {
        return connectionConfigGet("basicAuth", Boolean.FALSE);
    }

    // ---------- 生成配置 ----------

    private Properties buildProperties(String type) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getConnectionClusterURI());
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, getConnectionClientId(type));
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(10L));

        if (useSasl()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        }

        Protocol securityProtocol = getSecurityProtocol();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name());
        if (useSasl()) {
            String model;
            SaslMechanism mechanism = SaslMechanism.fromString(getSaslMechanism());
            props.put(SaslConfigs.SASL_MECHANISM, mechanism.getValue());
            switch (mechanism) {
                case PLAIN:
                    model = "org.apache.kafka.common.security.plain.PlainLoginModule";
                    break;
                case SCRAM_SHA_256:
                case SCRAM_SHA_512:
                    model = "org.apache.kafka.common.security.scram.ScramLoginModule";
                    break;
                case GSSAPI:
                    // kerberos
//                    model = "com.sun.security.auth.module.Krb5LoginModule";
//                    sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
//                    useKeyTab=true \
//                    storeKey=true \
//                    keyTab="/path/to/kafka_client.keytab" \
//                    principal="client/your.hostname@YOUR.REALM";
                default:
                    throw new IllegalArgumentException("Un-supported mechanism: " + mechanism.getValue());
            }
            props.put(SaslConfigs.SASL_JAAS_CONFIG, model +
                    " required" +
                    " username='" + getSaslUsername() +
                    "' password='" + getSaslPassword() + "';");
        }
        if (useSsl()) {
//            ssl.truststore.location=/path/to/kafka.client.truststore.jks
//            ssl.truststore.password=truststore_password
//            ssl.keystore.location=/path/to/kafka.client.keystore.jks
//            ssl.keystore.password=keystore_password
//            ssl.key.password=key_password
        }
        addRegistryConfig(props);
        return props;
    }

    public void addRegistryConfig(Properties props) {
        if (this.getConnectionSchemaRegister()) {
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            switch (getConnectionRegistrySchemaType()) {
                case "JSON":
                    props.put("value.serializer", io.confluent.kafka.serializers.KafkaJsonSerializer.class);
                    break;
                case "AVRO":
                    props.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
                    break;
                case "PROTOBUF":
                    props.put("value.serializer", io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer.class);
                    break;
                default:
                    props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
                    break;
            }
            props.put("schema.registry.url", "http://" + getConnectionSchemaRegisterUrl());
            if (getConnectionBasicAuth()) {
                props.put("basic.auth.credentials.source", getConnectionAuthCredentialsSource());
                props.put("basic.auth.user.info", getConnectionAuthUserName() + ":" + getConnectionAuthPassword());
            }
        }
    }

    public Properties buildAdminConfig() {
        String type = "Admin";
        Properties props = buildProperties(type);
        eachConnectionExtParams(props::put, "All", type);
        return props;
    }

    public Properties buildConsumerConfig(boolean isEarliest) {
        String type = "Consumer";
        Properties props = buildProperties(type);
        getConnectionSchemaMode().setDeserializer(this, props);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, isEarliest ? "earliest" : "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        eachConnectionExtParams(props::put, "All", type);
        return props;
    }

    public Properties buildDiscoverSchemaConfig(boolean isEarliest) {
        String type = "Consumer";
        Properties props = buildProperties(type);
        getConnectionSchemaMode().setDeserializer(this, props);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, isEarliest ? "earliest" : "latest");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 0);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        eachConnectionExtParams(props::put, "All", type);
        return props;
    }

    public Properties buildProducerConfig() {
        String type = "Producer";
        Properties props = buildProperties(type);
        KafkaSchemaMode mode;
        if (getConnectionSchemaRegister()) {
            mode = KafkaSchemaMode.valueOf("REGISTRY_" + getConnectionRegistrySchemaType());
        } else {
            mode = getNodeSchemaMode();
        }
        mode.setSerializer(this, props);

        KafkaAcksType ackType = getConnectionAcksType();
        switch (ackType) {
            case WRITE_MOST_ISR:
            case WRITE_ALL_ISR:
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                props.put(ProducerConfig.ACKS_CONFIG, ackType.getValue());
                break;
            default:
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
                props.put(ProducerConfig.ACKS_CONFIG, ackType.getValue());
                break;
        }
        Optional.ofNullable(getConnectionCompressionType()).ifPresent(compressionType -> props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType));

        eachConnectionExtParams(props::put, "All", type);
        return props;
    }

    // ---------- 静态方法 ----------

    public static KafkaConfig valueOf(TapConnectionContext context) {
        return new KafkaConfig(context);
    }
}
