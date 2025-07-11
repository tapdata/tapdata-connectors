package io.tapdata.kafka.schema_mode;

import io.tapdata.connector.utils.ConcurrentUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.KafkaEnhancedConnector;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Kafka 标准结构模式服务接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/3 17:00 Create
 */
public class StandardSchemaMode extends AbsSchemaMode {
    public StandardSchemaMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.STANDARD, kafkaService);
    }

    @Override
    public void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        Queue<String> toBeLoadTables = tables.stream().filter(Objects::nonNull).distinct().collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        List<TapTable> results = new LinkedList<>();
        try {
            String executorGroup = String.format("%s-discoverSchema", KafkaEnhancedConnector.PDK_ID);
            ConcurrentUtils.runWithQueue(kafkaService.getExecutorService(), executorGroup, toBeLoadTables, 20, table -> {
                TapTable sampleTable = new TapTable(table);
                kafkaService.<Long, Object>sampleValue(Collections.singletonList(table), null, record -> {
                    if (null != record) {
                        if (record.value() instanceof TapInsertRecordEvent) {
                            TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) record.value();
                            Map<String, Object> data = insertRecordEvent.getAfter();
                            KafkaUtils.data2TapTableFields(sampleTable, data);
                            return false;
                        }
                    }
                    return true;
                });
                results.add(sampleTable);
            });
            consumer.accept(results);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        Object value = consumerRecord.value();
        if (value instanceof TapEvent) {
            return (TapEvent) value;
        } else if (null == value) {
            return null;
        }
        throw new IllegalArgumentException(String.format("record type '%s'", value.getClass().getName()));
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent) {
        String topic = topic(table, tapEvent);
        Long ts = tapEvent.getTime();

        Map<String, Object> data;
        Headers headers = new RecordHeaders();
        if (tapEvent instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) tapEvent;
            recordEvent.setTableId(table.getId());
            data = recordEvent.getAfter();
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent recordEvent = (TapUpdateRecordEvent) tapEvent;
            recordEvent.setTableId(table.getId());
            data = recordEvent.getAfter();
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            TapDeleteRecordEvent recordEvent = (TapDeleteRecordEvent) tapEvent;
            recordEvent.setTableId(table.getId());
            data = recordEvent.getBefore();
        } else {
            throw new NotSupportedException(String.format("TapEvent type '%s'", tapEvent.getClass().getName()));
        }
        return Arrays.asList(new ProducerRecord<>(topic, null, ts, createKafkaKey(data, table), tapEvent, headers));
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
        List<String> tables = Collections.singletonList(table.getId());
        Integer limit = Optional.ofNullable(filter.getLimit()).orElse(1);
        AtomicInteger total = new AtomicInteger(0);
        FilterResults filterResults = new FilterResults();
        kafkaService.sampleValue(tables, null, consumerRecord -> {
            TapEvent tapEvent = toTapEvent(consumerRecord);
            if (tapEvent instanceof TapInsertRecordEvent) {
                filterResults.add(((TapInsertRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapUpdateRecordEvent) {
                filterResults.add(((TapUpdateRecordEvent) tapEvent).getBefore());
            } else if (tapEvent instanceof TapDeleteRecordEvent) {
                filterResults.add(((TapDeleteRecordEvent) tapEvent).getBefore());
            } else {
                return true;
            }
            return total.incrementAndGet() < limit;
        });
        consumer.accept(filterResults);
        throw new NotSupportedException(String.format("Advance filter '%s'", filter));
    }
}
