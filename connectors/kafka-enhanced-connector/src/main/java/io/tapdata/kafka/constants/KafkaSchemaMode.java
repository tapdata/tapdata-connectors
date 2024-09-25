package io.tapdata.kafka.constants;

import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.serialization.StandardDeserializer;
import io.tapdata.kafka.serialization.StandardSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

/**
 * Kafka 结构模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 18:13 Create
 */
public enum KafkaSchemaMode {
    STANDARD() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StandardSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StandardDeserializer.class.getName());
        }
    },
    ORIGINAL() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaConfig.getConnectionKeySerialization().getSerializer());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaConfig.getConnectionValueSerialization().getSerializer());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConfig.getConnectionKeySerialization().getDeserializer());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConfig.getConnectionValueSerialization().getDeserializer());
        }
    },
    ;

    public abstract void setSerializer(KafkaConfig kafkaConfig, Properties props);

    public abstract void setDeserializer(KafkaConfig kafkaConfig, Properties props);


    public static KafkaSchemaMode fromString(String mode) {
        for (KafkaSchemaMode schemaMode : KafkaSchemaMode.values()) {
            if (schemaMode.name().equalsIgnoreCase(mode)) {
                return schemaMode;
            }
        }
        return STANDARD;
    }

}
