package io.tapdata.common.sqlparser;

public enum Operate {
    INSERT, DELETE, UPDATE,
    ;

    public static Operate parse(String str) {
        return Operate.valueOf(str);
    }

}
