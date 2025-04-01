package io.tapdata.connector.starrocks.streamload;

public enum StarrocksTableType {
    Primary,
    Duplicate,
    Aggregate,
    Unique,
}
