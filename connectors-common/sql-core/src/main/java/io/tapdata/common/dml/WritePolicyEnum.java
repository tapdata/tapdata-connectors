package io.tapdata.common.dml;

public enum WritePolicyEnum {

    UPDATE_ON_EXISTS("update_on_exists"),
    IGNORE_ON_EXISTS("ignore_on_exists"),
    JUST_INSERT("just_insert"),
    IGNORE_ON_NONEXISTS("ignore_on_nonexists"),
    INSERT_ON_NONEXISTS("insert_on_nonexists"),
    LOG_ON_NONEXISTS("log_on_nonexists");

    private final String policyName;

    WritePolicyEnum(String policyName) {
        this.policyName = policyName;
    }

    public String getPolicyName() {
        return policyName;
    }

}
