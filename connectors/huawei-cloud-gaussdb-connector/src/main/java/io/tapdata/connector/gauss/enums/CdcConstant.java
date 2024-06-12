package io.tapdata.connector.gauss.enums;

public class CdcConstant {
    public static final String HEART_TAG = "H";
    public static final String BEGIN_TAG = "B";
    public static final String INSERT_TAG = "I";
    public static final String UPDATE_TAG = "U";
    public static final String DELETE_TAG = "D";
    public static final String COMMIT_TAG = "C";

    public static final int CDC_MAX_BATCH_SIZE = 1000;
    public static final int CDC_DEFAULT_BATCH_SIZE = 100;
    public static final int CDC_MIN_BATCH_SIZE = 0;

    public static final int BYTES_COUNT_BUFF_START = 4;
    public static final int BYTES_COUNT_LSN = 8;
    public static final int BYTES_COUNT_EVENT_TYPE = 1;


    public static final int BYTES_VALUE_OF_NULL = 0xFFFFFFFF;
    public static final int BYTES_VALUE_OF_EMPTY_CHAR = 0;





    public static final String GAUSS_DB_SLOT_TAG = "open_gauss_slot";
    public static final String GAUSS_DB_SLOT_SFF = "gauss_slot_";
    public static final String GAUSS_DB_SLOT_DEFAULT_PLUGIN = "mppdb_decoding";


    public static final int CDC_FLUSH_LOGIC_LSN_DEFAULT = 10 * 60 * 1000; //ms
    public static final int CDC_FLUSH_LOGIC_LSN_MIN = 10 * 60 * 1000; //ms
    public static final int CDC_FLUSH_LOGIC_LSN_MAX = 100 * 60 * 1000; //ms
}
