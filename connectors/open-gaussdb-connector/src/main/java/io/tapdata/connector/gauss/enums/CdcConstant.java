package io.tapdata.connector.gauss.enums;

public class CdcConstant {
    public static final String HEART_TAG = "H";
    public static final String HEART_TAG_LOW = "h";

    public static final int CDC_MAX_BATCH_SIZE = 1000;
    public static final int CDC_DEFAULT_BATCH_SIZE = 100;
    public static final int CDC_MIN_BATCH_SIZE = 0;

    public static final int BYTES_COUNT_BUFF_START = 4;
    public static final int BYTES_COUNT_LSN = 8;
    public static final int BYTES_COUNT_EVENT_TYPE = 1;

}
