package io.tapdata.kafka.service;

import io.tapdata.connector.utils.AsyncBatchPusher;
import io.tapdata.connector.utils.BatchPusher;
import io.tapdata.connector.utils.ConcurrentUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.exception.TapPdkConfigEx;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.KafkaEnhancedConnector;
import io.tapdata.kafka.constants.KafkaConcurrentReadMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.data.KafkaOffset;
import io.tapdata.kafka.utils.KafkaOffsetUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * 数据消费服务
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/7 17:01 Create
 */
public class KafkaConsumerService implements AutoCloseable {

    private final IKafkaService service;
    private final AtomicBoolean stopping;
    private final KafkaConfig config;
    private final long batchMaxDelay;
    private final int maxConcurrentSize;
    private final Duration timeout;

    public KafkaConsumerService(IKafkaService service, AtomicBoolean stopping) {
        this.service = service;
        this.stopping = stopping;
        this.config = service.getConfig();
        this.batchMaxDelay = config.getNodeBatchMaxDelay();
        this.maxConcurrentSize = Math.max(config.getNodeMaxConcurrentSize(), 1);
        this.timeout = Duration.ofMillis(batchMaxDelay);
    }

    public void start(KafkaOffset offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Exception {
        KafkaConcurrentReadMode nodeConcurrentReadMode = config.getNodeConcurrentReadMode();
        switch (nodeConcurrentReadMode) {
            case SINGLE:
                runWithSingleMode(offset, batchSize, consumer);
                break;
            case TOPIC:
                runWithTopicMode(offset, batchSize, consumer);
                break;
            case PARTITIONS:
                runWithPartitionMode(offset, batchSize, consumer);
                break;
            default:
                throw new TapPdkConfigEx(String.format("not supported concurrentReadMode of type '%s'", nodeConcurrentReadMode), KafkaEnhancedConnector.PDK_ID);
        }
    }

    private void runWithSingleMode(KafkaOffset offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Exception {
        boolean isEarliest = true;
        try (KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(config.buildConsumerConfig(isEarliest))) {
            // 设置断点信息
            KafkaOffsetUtils.setKafkaOffset(kafkaConsumer, offset);

            try (BatchPusher<TapEvent> batchPusher = BatchPusher.<TapEvent>create(tapEvents -> {
                KafkaOffset cloneKafkaOffset = offset.clone();
                consumer.accept(tapEvents, cloneKafkaOffset);
            }).batchSize(batchSize).maxDelay(batchMaxDelay)) {
                // 将初始化的 offset 推送到目标，让指定时间的增量任务下次启动时拿到 offset
                sendHeartBeatEvent(consumer, offset);

                // 消费数据
                while (!stopping.get()) {
                    ConsumerRecords<Object, Object> consumerRecords = kafkaConsumer.poll(timeout);
                    if (consumerRecords.isEmpty()) {
                        batchPusher.checkAndSummit();
                    } else {
                        for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                            offset.setOffset(consumerRecord); // 推进 offset
                            TapEvent event = service.getSchemaModeService().toTapEvent(consumerRecord);
                            batchPusher.add(event);
                        }
                    }
                }
            }
        }
    }

    /**
     * 按主题并发消费
     *
     * @param offset    位点
     * @param batchSize 批处理大小
     * @param consumer  推送接口
     * @throws Exception 异常
     */
    private void runWithTopicMode(KafkaOffset offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Exception {
        int concurrentSize = Math.min(offset.size(), maxConcurrentSize);
        List<Map<TopicPartition, Long>> concurrentItems = initConcurrentItems(concurrentSize);
        AtomicInteger index = new AtomicInteger(0);
        offset.forEach((topic, topicOffset) -> {
                int concurrentIndex = index.getAndIncrement() % concurrentSize;
                Map<TopicPartition, Long> partitionOffsets = concurrentItems.get(concurrentIndex);
                topicOffset.forEach((partition, partitionOffset) -> partitionOffsets.put(new TopicPartition(topic, partition), partitionOffset));
            }
        );

        run(offset, concurrentItems, batchSize, consumer);
    }

    /**
     * 按分区并发消费
     *
     * @param offset    位点
     * @param batchSize 批处理大小
     * @param consumer  推送接口
     * @throws Exception 异常
     */
    private void runWithPartitionMode(KafkaOffset offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Exception {
        Map<TopicPartition, Long> partitionOffsets = new LinkedHashMap<>();
        AtomicInteger index = new AtomicInteger(0);
        offset.forEach((topic, topicOffset) ->
            topicOffset.forEach((partition, partitionOffset) ->
                partitionOffsets.put(new TopicPartition(topic, partition), partitionOffset)
            )
        );

        int concurrentSize = Math.min(partitionOffsets.size(), maxConcurrentSize);
        List<Map<TopicPartition, Long>> concurrentItems = initConcurrentItems(concurrentSize);
        offset.forEach((topic, topicOffset) ->
            topicOffset.forEach((partition, partitionOffset) -> {
                    int concurrentIndex = index.getAndIncrement() % concurrentSize;
                    Map<TopicPartition, Long> offsets = concurrentItems.get(concurrentIndex);
                    offsets.put(new TopicPartition(topic, partition), partitionOffset);
                }
            )
        );
        run(offset, concurrentItems, batchSize, consumer);
    }

    private void run(KafkaOffset offset, List<Map<TopicPartition, Long>> concurrentItems, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Exception {
        String executorGroup = String.format("%s-%s-consumer", KafkaEnhancedConnector.PDK_ID, config.getStateMapFirstConnectorId());
        try (
            AsyncBatchPusher<ConsumerRecord<Object, Object>, TapEvent> batchPusher = AsyncBatchPusher.<ConsumerRecord<Object, Object>, TapEvent>create(
                String.format("%s-batchPusher", executorGroup),
                offset::setOffset, // offset 推进
                tapEvents -> {
                    KafkaOffset cloneKafkaOffset = offset.clone();
                    consumer.accept(tapEvents, cloneKafkaOffset);
                },
                () -> !stopping.get()
            ).batchSize(batchSize).maxDelay(batchMaxDelay)) {
            // 将初始化的 offset 推送到目标，让指定时间的增量任务下次启动时拿到 offset
            sendHeartBeatEvent(consumer, offset);
            service.getExecutorService().submit(batchPusher);

            // 并发消费数据
            ConcurrentUtils.runWithList(service.getExecutorService(), executorGroup, concurrentItems, (ex, index, partitionOffsets) -> {
                Properties properties = config.buildConsumerConfig(true);
                properties.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("%s-%s", executorGroup, index));
                try (KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(properties)) {
                    kafkaConsumer.assign(partitionOffsets.keySet());
                    partitionOffsets.forEach(kafkaConsumer::seek);

                    while (!stopping.get() && null == ex.get()) {
                        ConsumerRecords<Object, Object> consumerRecords = kafkaConsumer.poll(timeout);
                        if (consumerRecords.isEmpty()) continue;

                        for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                            TapEvent event = service.getSchemaModeService().toTapEvent(consumerRecord);
                            batchPusher.add(consumerRecord, event);
                        }
                    }
                }
            });
        }
    }

    private void sendHeartBeatEvent(BiConsumer<List<TapEvent>, Object> consumer, KafkaOffset offset) {
        HeartbeatEvent event = new HeartbeatEvent();
        event.setTime(System.currentTimeMillis());
        List<TapEvent> tapEvents = new ArrayList<>();
        tapEvents.add(event);
        consumer.accept(tapEvents, offset);
    }

    private List<Map<TopicPartition, Long>> initConcurrentItems(int size) {
        List<Map<TopicPartition, Long>> concurrentItems = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            concurrentItems.add(new LinkedHashMap<>());
        }
        return concurrentItems;
    }

    @Override
    public void close() throws Exception {

    }
}
