package io.tapdata.connector.kafka.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class AbstractConfigurationTest {

    @Nested
    class Build{
        AbstractConfiguration abstractConfiguration;
        KafkaConfig kafkaConfig;
        @BeforeEach
        void init(){
            abstractConfiguration = mock(AdminConfiguration.class);
            kafkaConfig = new KafkaConfig();
            kafkaConfig.setMqUsername("root");
            kafkaConfig.setMqPassword("123456");
            kafkaConfig.setKafkaSaslMechanism("PLAIN");
            kafkaConfig.setKrb5(false);
            Map<String, Object> configMap = new HashMap<>();
            ReflectionTestUtils.setField(abstractConfiguration,"kafkaConfig",kafkaConfig);
            ReflectionTestUtils.setField(abstractConfiguration,"configMap",configMap);
            doCallRealMethod().when(abstractConfiguration).build();
        }
        @Test
        void testBuildSecurityProtocolIsSASL_SSL(){
            kafkaConfig.setSecurityProtocol("SASL_SSL");
            Map<String, Object> actualData = abstractConfiguration.build();
            Assertions.assertTrue("SASL_SSL".equals(actualData.get("security.protocol").toString()));
        }

        @Test
        void testBuildSecurityProtocolIsSASL_PLAINTEXT(){
            kafkaConfig.setSecurityProtocol("SASL_PLAINTEXT");
            Map<String, Object> actualData = abstractConfiguration.build();
            Assertions.assertTrue("SASL_PLAINTEXT".equals(actualData.get("security.protocol").toString()));
        }
    }
}
