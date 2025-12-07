package io.tapdata.kafka.constants;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.serialization.JsonDeserializer;
import io.tapdata.kafka.serialization.JsonSerializer;
import io.tapdata.kafka.serialization.StandardDeserializer;
import io.tapdata.kafka.serialization.StandardSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

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
    CANAL() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        }
    },
    DEBEZIUM() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        }
    },
    FLINK_CDC() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        }
    },
    REGISTRY_AVRO() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        }
    },
    REGISTRY_PROTOBUF() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        }
    },
    REGISTRY_JSON() {
        @Override
        public void setSerializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        }

        @Override
        public void setDeserializer(KafkaConfig kafkaConfig, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
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
