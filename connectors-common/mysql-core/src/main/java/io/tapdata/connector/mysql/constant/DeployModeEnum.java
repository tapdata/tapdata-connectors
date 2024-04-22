package io.tapdata.connector.mysql.constant;


import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

/**
 * @author lemon
 */

public enum DeployModeEnum {

    // redis three mode
    STANDALONE("standalone"),
    MASTER_SLAVE("master-slave");
    private final String mode;

    DeployModeEnum(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
    public static final HashMap<String, DeployModeEnum> MODE_MAP = new HashMap();
    static {
        for (DeployModeEnum value : values()) {
            MODE_MAP.put(value.getMode(), value);
        }
    }

    public static DeployModeEnum fromString(String mode) {
        return MODE_MAP.get(mode);
    }
}
