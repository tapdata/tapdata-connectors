package io.tapdata.connector.kafka;

import io.tapdata.connector.kafka.config.KafkaConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Properties;

public class KafkaConnectorTest {

    @Nested
    class schemaRegisterBuild{

        KafkaConnector kafkaConnector;
        KafkaConfig kafkaConfig;
        @BeforeEach
        void init(){
            kafkaConnector = new KafkaConnector();
            kafkaConfig = new KafkaConfig();
            kafkaConfig.setMqUsername("root");
            kafkaConfig.setMqPassword("123456");
            kafkaConfig.setKafkaSaslMechanism("PLAIN");
            kafkaConfig.setKrb5(false);
            kafkaConfig.setNameSrvAddr("127.0.0.1:9092");
            ReflectionTestUtils.setField(kafkaConnector,"kafkaConfig",kafkaConfig);
        }
        @Test
        void testBuildSecurityProtocolIsSASL_SSL(){
            kafkaConfig.setSecurityProtocol("SASL_SSL");
            ReflectionTestUtils.invokeMethod(kafkaConnector,"schemaRegisterBuild");
            Properties actualData = (Properties) ReflectionTestUtils.getField(kafkaConnector,"properties");
            Assertions.assertTrue("SASL_SSL".equals(actualData.get("security.protocol").toString()));

        }

        @Test
        void testBuildSecurityProtocolIsSASL_PLAINTEXT(){
            kafkaConfig.setSecurityProtocol("SASL_PLAINTEXT");
            ReflectionTestUtils.invokeMethod(kafkaConnector,"schemaRegisterBuild");
            Properties actualData = (Properties) ReflectionTestUtils.getField(kafkaConnector,"properties");
            Assertions.assertTrue("SASL_PLAINTEXT".equals(actualData.get("security.protocol").toString()));
        }




    }
}
