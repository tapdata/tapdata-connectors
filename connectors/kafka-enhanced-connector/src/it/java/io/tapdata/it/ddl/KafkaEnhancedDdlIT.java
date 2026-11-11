package io.tapdata.it.ddl;

import io.tapdata.kafka.KafkaEnhancedConnector;
import io.tapdata.kafka.KafkaEnhancedCoreConnector;

public class KafkaEnhancedDdlIT extends KafkaSchemaDdlIT {
    @Override
    protected KafkaEnhancedCoreConnector connector() {
        return new KafkaEnhancedConnector();
    }
}
