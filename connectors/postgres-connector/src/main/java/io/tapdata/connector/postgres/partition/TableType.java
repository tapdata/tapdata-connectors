package io.tapdata.connector.postgres.partition;

public class TableType {
    public static final String PARTITIONED_TABLE = "Partitioned Table";
    public static final String CHILD_TABLE = "Child Table";
    public static final String PARENT_TABLE = "Parent Table";
    public static final String REGULAR_TABLE = "Regular Table";

    public static final String INHERIT = "Inherit";
    public static final String RANGE = "Range";
    public static final String HASH = "Hash";
    public static final String LIST = "List";
    public static final String UN_KNOW = "Unknow";

    public static final String KEY_TABLE_TYPE = "table_type";
    public static final String KEY_PARENT_TABLE = "parent_table";
    public static final String KEY_PARTITION_TABLE = "partition_table";
    public static final String KEY_TABLE_NAME = "table_name";
    public static final String KEY_CHECK_OR_PARTITION_RULE = "check_or_partition_rule";
    public static final String KEY_PARTITION_TYPE = "partition_type";
    public static final String KEY_PARTITION_BOUND = "partition_bound";

}