package io.tapdata.it.ddl;

import io.tapdata.kafka.KafkaAvroConnector;
import io.tapdata.kafka.KafkaEnhancedCoreConnector;

public class KafkaAvroDdlIT extends KafkaSchemaDdlIT {
    @Override
    protected KafkaEnhancedCoreConnector connector() {
        return new KafkaAvroConnector();
    }
}
