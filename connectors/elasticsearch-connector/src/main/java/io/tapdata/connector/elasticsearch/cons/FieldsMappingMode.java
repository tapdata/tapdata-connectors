package io.tapdata.connector.elasticsearch.cons;

/**
 * 字段映射模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/8 18:19 Create
 */
public enum FieldsMappingMode {
    AUTO,
    SCHEMA,
    ;

    public static FieldsMappingMode fromString(String mode) {
        if (null != mode) {
            switch (mode.trim().toUpperCase()) {
                case "AUTO":
                    return AUTO;
                case "SCHEMA":
                    return SCHEMA;
                default:
                    break;
            }
        }
        return SCHEMA;
    }
}
