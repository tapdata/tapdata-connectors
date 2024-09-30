package io.tapdata.kafka;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Kafka 连接器服务接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/30 11:40 Create
 */
public interface IKafkaService extends AutoCloseable {

    Log getLog();

    KafkaConfig getConfig();

    ExecutorService getExecutorService();

    KafkaProducer<Object, Object> getProducer();

    IKafkaAdminService getAdminService();

    AbsSchemaMode getSchemaModeService();

    void tableNames(int batchSize, Consumer<List<String>> consumer);

    void discoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer);

    <K, V> void sampleValue(List<String> tables, Object offset, Predicate<ConsumerRecord<K, V>> callback);

    long batchCount(TapTable table);

    void batchRead(TapTable table, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer);

    Object timestampToStreamOffset(Long startTime) throws Throwable;

    void streamRead(List<String> tables, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer);

    void writeRecord(List<TapRecordEvent> recordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer);

    CreateTableOptions createTable(TapCreateTableEvent tapCreateTableEvent);

    void deleteTable(TapDropTableEvent tapDropTableEvent);

    void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer);
}
