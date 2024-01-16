package io.tapdata.connector.tdengine.subscribe;

import com.taosdata.jdbc.tmq.*;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.tdengine.TDengineJdbcContext;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;

public class TDengineSubscribe {

    private final TDengineJdbcContext tdengineJdbcContext;
    private final Log tapLogger;
    private Object offsetState;
    private Map<String, String> timestampColumnMap; //pdk tableMap in streamRead
    private List<String> tableList; //tableName list
    private int recordSize;
    private StreamReadConsumer consumer;


    public TDengineSubscribe(TDengineJdbcContext tdengineJdbcContext, Log tapLogger) {
        this.tdengineJdbcContext = tdengineJdbcContext;
        this.tapLogger = tapLogger;
    }

    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap,
                     Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.tableList = tableList;
        this.timestampColumnMap = tableList.stream().collect(Collectors.toMap(v -> v, v -> tableMap.get(v).getNameFieldMap().keySet().stream().findFirst()
                .orElseThrow(() -> new RuntimeException("table name field is null"))));
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
    }

    public void subscribe(Supplier<Boolean> isAlive) {

        try {
            CommonDbConfig config = tdengineJdbcContext.getConfig();

            // create consumer
            Properties properties = new Properties();
            if (((TDengineConfig) config).getSupportWebSocket()) {
                properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, String.format("%s:%s", config.getHost(), config.getPort()));
                properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
                properties.setProperty(TMQConstants.CONNECT_USER, config.getUser());
                properties.setProperty(TMQConstants.CONNECT_PASS, config.getPassword());
            } else {
                properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, String.format("%s:%s", config.getHost(), ((TDengineConfig) config).getOriginPort()));
                properties.setProperty(TMQConstants.CONNECT_TYPE, "jni");
            }
            properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, Boolean.TRUE.toString());
            properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, Boolean.FALSE.toString());
            properties.setProperty(TMQConstants.GROUP_ID, "test_group_id");
            properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER,
                    "io.tapdata.connector.tdengine.subscribe.TDengineResultDeserializer");

            List<String> topicList = tableList.stream().map(v -> "tap_topic_" + v).collect(Collectors.toList());
            // poll data
            try (TaosConsumer<Map<String, Object>> taosConsumer = new TaosConsumer<>(properties)) {
                taosConsumer.subscribe(topicList);
                consumer.streamReadStarted();
                List<TapEvent> tapEvents = new ArrayList<>();
                while (isAlive.get()) {
                    ConsumerRecords<Map<String, Object>> records = taosConsumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        TapSimplify.sleep(1000);
                    } else {
                        for (ConsumerRecord<Map<String, Object>> record : records) {
                            Map<String, Object> recordValue = record.value();
                            String tableName = getTableName(record.getTopic());
                            TapInsertRecordEvent tapInsertRecordEvent = insertRecordEvent(recordValue, tableName);
                            String timeString = recordValue.get(timestampColumnMap.get(tableName)).toString();
                            tapInsertRecordEvent.setReferenceTime(Timestamp.valueOf(timeString).getTime());
                            tapEvents.add(tapInsertRecordEvent);
                            if (tapEvents.size() >= recordSize) {
                                consumer.accept(tapEvents, offsetState);
                                taosConsumer.commitSync();
                                tapEvents.clear();
                            }
                        }
                    }
                }
                if (EmptyKit.isNotEmpty(tapEvents)) {
                    consumer.accept(tapEvents, offsetState);
                    taosConsumer.commitSync();
                }
                consumer.streamReadEnded();
            }
        } catch (SQLException e) {
            tapLogger.error("Table data sync error: {}", e.getMessage(), e);
        }
    }

    private String getTableName(String topic) {
        if (EmptyKit.isEmpty(topic)) {
            return null;
        }
        if (topic.startsWith("tap_topic_")) {
            return topic.substring(10);
        }
        return null;
    }

    public Optional<TopicPartition> getTopicPartition(ConsumerRecords<Map<String, Object>> consumerRecords) throws NoSuchFieldException, IllegalAccessException {
        Field field = consumerRecords.getClass().getDeclaredField("records");
        field.setAccessible(Boolean.TRUE);
        Map<TopicPartition, List<Map<String, Object>>> records = (Map<TopicPartition, List<Map<String, Object>>>) field.get(consumerRecords);
        Set<TopicPartition> topicPartitions = records.keySet();
        return topicPartitions.stream().filter(Objects::nonNull).findFirst();
    }

}
