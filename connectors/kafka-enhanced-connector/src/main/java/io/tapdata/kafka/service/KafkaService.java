package io.tapdata.kafka.service;

import io.tapdata.connector.error.KafkaErrorCodes;
import io.tapdata.connector.utils.AsyncBatchPusher;
import io.tapdata.connector.utils.ErrorHelper;
import io.tapdata.connector.utils.ConcurrentUtils;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.mapping.TapEntry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.exception.TapCodeException;
import io.tapdata.exception.TapPdkConfigEx;
import io.tapdata.exception.TapPdkTerminateByServerEx;
import io.tapdata.exception.TapRuntimeException;
import io.tapdata.exception.runtime.TapPdkSkippableDataEx;
import io.tapdata.kafka.*;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.constants.ProducerRecordWrapper;
import io.tapdata.kafka.data.KafkaOffset;
import io.tapdata.kafka.data.KafkaTopicOffset;
import io.tapdata.kafka.utils.KafkaBatchReadOffsetUtils;
import io.tapdata.kafka.utils.KafkaOffsetUtils;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Kafka 连接器服务实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/27 14:57 Create
 */
public class KafkaService implements IKafkaService {
    private final Log logger;
    private final AtomicBoolean stopping;
    private final KafkaConfig config;
    private final ExecutorService executorService;
    private final AbsSchemaMode schemaModeService;
    private KafkaProducer<Object, Object> kafkaProducer;
    private IKafkaAdminService adminService;

    public KafkaService(KafkaConfig config, AtomicBoolean stopping) {
        this.config = config;
        this.stopping = stopping;

        this.logger = config.tapConnectionContext().getLog();
        this.executorService = Executors.newFixedThreadPool(200, r -> {
            Thread thread = new Thread(r);
            thread.setName(String.format("%s-executor-%s", KafkaEnhancedConnector.PDK_ID, thread.getId()));
            return thread;
        });
        if (config.getConnectionSchemaRegister()) {
            this.schemaModeService = AbsSchemaMode.create(KafkaSchemaMode.fromString("REGISTRY_" + config.getConnectionRegistrySchemaType()), this);
        } else {
            this.schemaModeService = null != config.getNodeSchemaMode() ? AbsSchemaMode.create(config.getNodeSchemaMode(), this) : AbsSchemaMode.create(config.getConnectionSchemaMode(), this);
        }
    }

    @Override
    public Log getLog() {
        return logger;
    }

    @Override
    public KafkaConfig getConfig() {
        return config;
    }

    @Override
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public synchronized KafkaProducer<Object, Object> getProducer() {
        if (kafkaProducer == null) {
            logger.info("Creating producer for {}", KafkaEnhancedConnector.PDK_ID);
            kafkaProducer = new KafkaProducer<>(config.buildProducerConfig());
        }
        return kafkaProducer;
    }

    @Override
    public synchronized IKafkaAdminService getAdminService() {
        if (adminService == null) {
            logger.info("Creating adminService for {}", KafkaEnhancedConnector.PDK_ID);
            adminService = new KafkaAdminService(config, logger);
        }
        return adminService;
    }

    @Override
    public AbsSchemaMode getSchemaModeService() {
        return this.schemaModeService;
    }

    @Override
    public void tableNames(int batchSize, Consumer<List<String>> consumer) {
        try {
            List<String> tableNames = new ArrayList<>();
            Set<String> topics = getAdminService().listTopics();
            int i = 0;
            for (String topic : topics) {
                tableNames.add(topic);
                if (0 != i && i % batchSize == 0) {
                    consumer.accept(tableNames);
                    tableNames = new ArrayList<>();
                }
                i++;
            }
            if (!tableNames.isEmpty()) {
                consumer.accept(tableNames);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void discoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        if (null == tables || tables.isEmpty()) return;
        schemaModeService.discoverSchema(this, tables, tableSize, consumer);
    }

    @Override
    public long batchCount(TapTable table) {
        boolean isEarliest = true;
        String topic = table.getName();
        try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(config.buildConsumerConfig(isEarliest))) {
            Collection<TopicPartition> topicPartitions = getAdminService().getTopicPartitions(Collections.singleton(topic));

            long beginTotals = 0;
            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToBeginning(topicPartitions);
            for (TopicPartition topicPartition : topicPartitions) {
                long position = kafkaConsumer.position(topicPartition);
                beginTotals += position;
            }

            long endTotals = 0;
            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToEnd(topicPartitions);
            for (TopicPartition topicPartition : topicPartitions) {
                long position = kafkaConsumer.position(topicPartition);
                endTotals += position;
            }

            return endTotals - beginTotals;
        } catch (InterruptedException | org.apache.kafka.common.errors.InterruptException e) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (TapRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TapPdkTerminateByServerEx(KafkaEnhancedConnector.PDK_ID, e);
        }
    }

    @Override
    public void batchRead(TapTable table, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) {
        try {
            // 参数设置
            boolean isEarliest = true;
            String topic = table.getId();
            int concurrentSize = config.getNodeMaxConcurrentSize();
            long batchMaxDelay = config.getNodeBatchMaxDelay();
            Duration timeout = Duration.ofMillis(batchMaxDelay);

            // offset 设置
            KafkaTopicOffset endTopicOffsets;
            KafkaTopicOffset startTopicOffsets;
            Queue<TapEntry<Integer, Long>> concurrentQueue = new ConcurrentLinkedQueue<>();
            try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(config.buildConsumerConfig(isEarliest))) {
                Collection<TopicPartition> topicPartitions = getAdminService().getTopicPartitions(Collections.singleton(topic));
                endTopicOffsets = KafkaBatchReadOffsetUtils.topicOffsetFromStateMap(topic, config, kafkaConsumer, topicPartitions);

                // Get start offsets - use KafkaTopicOffset instead of List to maintain partition number mapping
                if (offset instanceof KafkaTopicOffset) {
                    startTopicOffsets = (KafkaTopicOffset) offset;
                } else if (offset instanceof List) {
                    // Legacy support for List offset - convert to KafkaTopicOffset
                    List<Long> offsetList = (List<Long>) offset;
                    startTopicOffsets = new KafkaTopicOffset();
                    for (int i = 0; i < offsetList.size(); i++) {
                        startTopicOffsets.setOffset(i, offsetList.get(i));
                    }
                } else {
                    // Get begin offsets from Kafka
                    KafkaOffset kafkaOffset = KafkaOffsetUtils.getKafkaOffset(kafkaConsumer, topicPartitions, isEarliest);
                    startTopicOffsets = kafkaOffset.get(topic);
                }

                // Build concurrent queue with actual partition numbers
                for (Map.Entry<Integer, Long> entry : startTopicOffsets.entrySet()) {
                    concurrentQueue.add(new TapEntry<>(entry.getKey(), entry.getValue()));
                }
            }
            List<TapEntry<String, Function<Object, Object>>> fieldTypeConverts = KafkaUtils.setFieldTypeConvert(table, new ArrayList<>());
            AtomicInteger index = new AtomicInteger(0);
            AtomicReference<Exception> exception = new AtomicReference<>();
            String executorGroup = String.format("%s-%s-batchRead", KafkaEnhancedConnector.PDK_ID, config.getStateMapFirstConnectorId());
            try (AsyncBatchPusher<ConsumerRecord<Object, Object>, TapEvent> batchPusher = AsyncBatchPusher.<ConsumerRecord<Object, Object>, TapEvent>create(
                exception,
                String.format("%s-batchPusher", executorGroup),
                consumerRecord -> {
                },
                tapEvents -> consumer.accept(tapEvents, null),
                () -> !stopping.get()
            ).batchSize(batchSize).maxDelay(batchMaxDelay)) {
                getExecutorService().execute(batchPusher); // 开始推送数据

                // 并发消费数据
                ConcurrentUtils.runWithQueue(getExecutorService(), exception, executorGroup, concurrentQueue, concurrentSize, stopping::get, concurrentItem -> {
                    int partition = concurrentItem.getKey();
                    Long endOffset = endTopicOffsets.get(partition);
                    if (null == endOffset) {
                        logger.warn("not found end offset with topic partition {}-{}", topic, partition);
                        return;
                    } else {
                        logger.info("end offset with topic partition {}-{}: start={}, end={}", topic, partition, concurrentItem.getValue(), endOffset);
                    }

                    Properties properties = config.buildConsumerConfig(isEarliest);
                    properties.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("%s-%s", executorGroup, index.incrementAndGet()));
                    try (KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(properties)) {
                        TopicPartition topicPartition = new TopicPartition(topic, partition);

                        // 如果没有数据，则跳过
                        KafkaOffset currEndOffset = KafkaOffsetUtils.getKafkaOffset(kafkaConsumer, List.of(topicPartition), true);
                        if (Optional.of(currEndOffset)
                            .map(m -> m.get(topic))
                            .map(o -> o.get(partition))
                            .map(l -> endOffset <= l)
                            .orElse(true)
                        ) {
                            logger.warn("not found any data with topic partition {}-{}, earliest offset {}", topic, partition, currEndOffset);
                            return;
                        }

                        kafkaConsumer.assign(Collections.singleton(topicPartition));
                        kafkaConsumer.seek(topicPartition, concurrentItem.getValue());

                        while (!stopping.get() && null == exception.get()) {
                            ConsumerRecords<Object, Object> consumerRecords = kafkaConsumer.poll(timeout);
                            if (consumerRecords.isEmpty()) continue;

                            for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                                TapEvent event = schemaModeService.toTapEvent(consumerRecord);
                                if (TapUnknownRecordEvent.TYPE == event.getType()) {
                                    TapUnknownRecordEvent unknownRecordEvent = (TapUnknownRecordEvent) event;
                                    String errorMsg = String.format("unknown event %s(%d-%d): %s", topic, consumerRecord.partition(), consumerRecord.offset(), unknownRecordEvent.getData());
                                    throw new TapPdkSkippableDataEx(errorMsg, KafkaEnhancedConnector.PDK_ID);
                                }
                                KafkaUtils.convertWithFieldType(fieldTypeConverts, event);
                                batchPusher.add(consumerRecord, event);

                                if (consumerRecord.offset() + 1 >= endOffset) {
                                    logger.info("Partition {}-{} batch read completed at offset {}", topic, partition, consumerRecord.offset());
                                    return; // 全量完成
                                }
                            }
                        }
                    }
                });
            }
        } catch (InterruptedException | org.apache.kafka.common.errors.InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (TapRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TapPdkTerminateByServerEx(KafkaEnhancedConnector.PDK_ID, e);
        }
    }

    @Override
    public Object timestampToStreamOffset(Long startTime) throws Throwable {
        try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(config.buildConsumerConfig(true))) {
            Collection<String> allTables = getAllSyncTables();
            Collection<TopicPartition> topicPartitions = getAdminService().getTopicPartitions(allTables);
            KafkaOffset streamOffset = KafkaOffsetUtils.timestamp2Offset(kafkaConsumer, topicPartitions, startTime);
            KafkaBatchReadOffsetUtils.toStateMap(config, streamOffset);
            return streamOffset;
        }
    }

    @Override
    public void streamRead(List<String> tables, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) {
        try {
            KafkaOffset streamOffset = Optional.ofNullable(offset).map(o -> {
                if (offset instanceof KafkaOffset) {
                    return (KafkaOffset) o;
                }
                throw new TapPdkConfigEx("streamOffset must be of type KafkaOffset: " + o.getClass().getName(), KafkaEnhancedConnector.PDK_ID);
            }).orElseThrow(() -> new TapPdkConfigEx("streamRead offset is null", KafkaEnhancedConnector.PDK_ID));

            // 补全分区信息（如：全量过程为主题添加了分区）
            KafkaOffsetUtils.fillPartitions(this, tables, streamOffset, true);

            BiConsumer<List<TapEvent>, Object> convertConsumer = getFieldTypeConverterConsumer(consumer);

            try (KafkaConsumerService consumerService = new KafkaConsumerService(this, stopping)) {
                consumerService.start(streamOffset, batchSize, convertConsumer);
            }
        } catch (InterruptedException | org.apache.kafka.common.errors.InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (TapRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TapPdkTerminateByServerEx(KafkaEnhancedConnector.PDK_ID, e);
        }
    }

    @Override
    public <K, V> void sampleValue(List<String> tables, Object offset, Predicate<ConsumerRecord<K, V>> callback) {
        boolean isEarliest = true;
        long batchMaxDelay = config.getNodeBatchMaxDelay();
        Duration timeout = Duration.ofMillis(batchMaxDelay);

        try (KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(config.buildDiscoverSchemaConfig(isEarliest))) {
            // 设置断点信息
            Collection<TopicPartition> topicPartitions = getAdminService().getTopicPartitions(tables);
            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToBeginning(topicPartitions);

            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(timeout);
            if (null == consumerRecords || consumerRecords.isEmpty()) return;

            for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                if (!callback.test(consumerRecord)) {
                    return;
                }
            }
        } catch (InterruptedException | org.apache.kafka.common.errors.InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (TapRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TapPdkTerminateByServerEx(KafkaEnhancedConnector.PDK_ID, e);
        }
    }

    @Override
    public void writeRecord(List<TapRecordEvent> recordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();

        List<ProducerRecordWrapper> producerRecords = new ArrayList<>();
        for (TapRecordEvent recordEvent : recordEvents) {
            List<ProducerRecord<Object, Object>> fromTapEvents = schemaModeService.fromTapEvent(table, recordEvent);
            if (null != fromTapEvents && !fromTapEvents.isEmpty()) {
                for (ProducerRecord<Object, Object> fromTapEvent : fromTapEvents) {
                    producerRecords.add(new ProducerRecordWrapper(recordEvent, fromTapEvent));
                }
            }
        }

        CountDownLatch latch = new CountDownLatch(producerRecords.size());
        AtomicReference<RuntimeException> sendEx = new AtomicReference<>();
        for (ProducerRecordWrapper producerRecord : producerRecords) {
            getProducer().send(producerRecord.getProducerRecord(), (metadata, exception) -> {
                try {
                    TapRecordEvent recordEvent = producerRecord.getRecordEvent();
                    if (exception != null) {
                        listResult.addError(recordEvent, exception);
                        sendEx.set(new TapCodeException(KafkaErrorCodes.INVALID_TOPIC, exception.getMessage(), exception).dynamicDescriptionParameters(producerRecord.getProducerRecord().topic()));
                    }

                    if (recordEvent instanceof TapInsertRecordEvent) {
                        insert.incrementAndGet();
                    } else if (recordEvent instanceof TapUpdateRecordEvent) {
                        update.incrementAndGet();
                    } else if (recordEvent instanceof TapDeleteRecordEvent) {
                        delete.incrementAndGet();
                    } else {
                        logger.error("Unexpected record type: {}", recordEvent.getClass().getName());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            while (!stopping.get()) {
                if (latch.await(500L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
            if (null != sendEx.get()) {
                throw sendEx.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
        }
    }

    @Override
    public CreateTableOptions createTable(TapCreateTableEvent tapCreateTableEvent) {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        short replicasSize = config.getNodeReplicasSize();
        int partitionNum = config.getNodePartitionSize();
        String topic = KafkaUtils.pickTopic(config, tapCreateTableEvent.getDatabase(), tapCreateTableEvent.getSchema(), tapCreateTableEvent.getTable());
        try {
            Set<String> existTopics = getAdminService().listTopics();
            if (!existTopics.contains(topic)) {
                createTableOptions.setTableExists(false);
                getAdminService().createTopic(Collections.singleton(topic), partitionNum, replicasSize);
            } else {
                List<TopicPartitionInfo> topicPartitionInfos = getAdminService().getTopicPartitionInfo(topic);
                int existTopicPartition = topicPartitionInfos.size();
                int existReplicasSize = topicPartitionInfos.get(0).replicas().size();
                if (existReplicasSize != replicasSize) {
                    logger.warn("cannot change the number of replicasSize of an existing table, will skip");
                }
                if (partitionNum < existTopicPartition) {
                    logger.warn("The number of partitions set is less than to the number of partitions of the existing table，will skip");
                } else if (partitionNum == existTopicPartition) {
                    logger.info("The number of partitions set is equal to the number of partitions of the existing table");
                } else {
                    getAdminService().increaseTopicPartitions(topic, partitionNum);
                }
                createTableOptions.setTableExists(true);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("Create topic " + topic + " failed, error: " + e.getMessage(), e);
        }
        return createTableOptions;
    }

    @Override
    public void deleteTable(TapDropTableEvent tapDropTableEvent) {
        String topic = KafkaUtils.pickTopic(config, tapDropTableEvent.getDatabase(), tapDropTableEvent.getSchema(), tapDropTableEvent.getTableId());
        try {
            logger.info("Deleting topic '{}'...", topic);
            getAdminService().dropTopics(Collections.singleton(topic));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("Delete topic " + topic + " failed, error: " + e.getMessage(), e);
        }
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
        schemaModeService.queryByAdvanceFilter(filter, table, consumer);
    }

    @Override
    public void close() throws Exception {
        if (stopping.compareAndSet(false, true)) {
            logger.info("Shutting down...");
        }
        ErrorHelper.closeAndCheck(() -> {
            if (null == executorService) return;

            executorService.shutdown();
            long currentTime = System.currentTimeMillis();
            AtomicInteger tryTotal = new AtomicInteger(0);
            while (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                if (tryTotal.getAndIncrement() % 5 == 0) {
                    logger.info("Waiting for executor shutdown: {}ms", System.currentTimeMillis() - currentTime);
                }
                executorService.shutdown();
            }
        }, adminService, kafkaProducer);
    }


    // ---------- 内部方法 ----------

    private Collection<String> getAllSyncTables() {
        Set<String> allTables = config.tableMap(tableMap -> {
            Set<String> list = new LinkedHashSet<>();
            Iterator<Entry<TapTable>> iterator = tableMap.iterator();
            while (iterator.hasNext()) {
                Entry<TapTable> entry = iterator.next();
                list.add(entry.getKey());
            }
            if (list.isEmpty()) return null;
            return list;
        }, null);

        if (null == allTables || allTables.isEmpty()) {
            throw new IllegalArgumentException("not found any topics");
        }
        return allTables;
    }

    private BiConsumer<List<TapEvent>, Object> getFieldTypeConverterConsumer(BiConsumer<List<TapEvent>, Object> consumer) {
        Map<String, List<TapEntry<String, Function<Object, Object>>>> allTableConvert = new ConcurrentHashMap<>();
        return (tapEvents, o) -> {
            tapEvents.forEach(tapEvent -> {
                if (tapEvent instanceof TapBaseEvent) {
                    TapBaseEvent baseEvent = (TapBaseEvent) tapEvent;
                    List<TapEntry<String, Function<Object, Object>>> converts = allTableConvert.computeIfAbsent(baseEvent.getTableId(), k -> {
                        TapTable tapTable = getConfig().tapConnectorContext().getTableMap().get(k);
                        return KafkaUtils.setFieldTypeConvert(tapTable, new ArrayList<>());
                    });
                    KafkaUtils.convertWithFieldType(converts, tapEvent);
                }
            });
            consumer.accept(tapEvents, o);
        };
    }

}
