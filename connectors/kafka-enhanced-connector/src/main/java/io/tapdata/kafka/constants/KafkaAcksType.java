package io.tapdata.kafka.constants;

/**
 * ACK 类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 14:53 Create
 */
public enum KafkaAcksType {
    NOT_SURE("0"),
    JUST_WRITE_MASTER("1"),
    WRITE_MOST_ISR("-1"),
    WRITE_ALL_ISR("all"),
    ;

    private final String value;

    KafkaAcksType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static KafkaAcksType fromValue(String value) {
        for (KafkaAcksType type : KafkaAcksType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        return WRITE_MOST_ISR;
    }
}
