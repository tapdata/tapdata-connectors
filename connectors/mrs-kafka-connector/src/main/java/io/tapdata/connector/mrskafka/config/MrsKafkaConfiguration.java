package io.tapdata.connector.mrskafka.config;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public interface MrsKafkaConfiguration {
    Map<String, Object> build();

    Set<String> getRawTopics();

    Pattern getPatternTopics();

    boolean hasRawTopics();

    Duration getPollTimeout();

    boolean isIgnoreInvalidRecord();

    MrsKafkaConfig getConfig();
}
