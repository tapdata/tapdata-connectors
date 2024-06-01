//package io.tapdata.connector.tidb.cdc.process.analyse.stream;
//
//import io.tapdata.connector.tidb.cdc.process.analyse.filter.ReadFilter;
//import io.tapdata.entity.event.TapEvent;
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public interface Accepter {
//    public static final int DEFAULT_BATCH_SIZE = 1000;
//    public static final int DEFAULT_BATCH_DELAY = 5;//s
//
//    public static Accepter create(Integer type, StreamReadConsumer cdcConsumer, ReadFilter readFilter, Map<String, Set<String>> blockFieldsMap, Map<String, TapTable> tapTableMap) {
////        Accepter accepter = null == type || type != ReadFilter.LOG_CDC_QUERY_READ_SOURCE ?
////                new NormalAccepter() : new TextAccepter(root, blockFieldsMap, tapTableMap);
//        Accepter accepter = new NormalAccepter();
//        accepter.setFilter(readFilter);
//        accepter.setStreamReader(cdcConsumer);
//        return accepter;
//    }
//
//    public void accept(String fullTableName, List<TapEvent> events, Object offset);
//
//    public void setStreamReader(StreamReadConsumer cdcConsumer);
//
//    public default void setFilter(ReadFilter readFilter){}
//
//    public default void close(){}
//}
