package io.tapdata.kafka.constants;

/**
 * Kafka 并发读方式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/30 15:09 Create
 */
public enum KafkaConcurrentReadMode {
    TOPIC,
    PARTITIONS,
    SINGLE,
    ;
}
