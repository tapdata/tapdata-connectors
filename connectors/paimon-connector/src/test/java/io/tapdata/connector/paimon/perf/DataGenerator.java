package io.tapdata.connector.paimon.perf;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 数据生成器 - 支持配置总数据量、写入QPS、主键重复率
 */
public class DataGenerator {
    private final Random random;
    private final AtomicLong idGenerator;
    private final Set<String> generatedIds;
    private final List<String> generatedIdList; // 用于快速随机访问
    private final int primaryKeyDuplicateRate;
    private final String tableName;

    public DataGenerator(int primaryKeyDuplicateRate) {
        this(primaryKeyDuplicateRate, "test_table");
    }

    public DataGenerator(int primaryKeyDuplicateRate, String tableName) {
        this.random = new Random(System.currentTimeMillis());
        this.idGenerator = new AtomicLong(1);
        this.generatedIds = new HashSet<>();
        this.generatedIdList = new ArrayList<>();
        this.primaryKeyDuplicateRate = Math.max(0, Math.min(100, primaryKeyDuplicateRate));
        this.tableName = tableName;
    }

    /**
     * 生成单个记录
     */
    public Map<String, Object> generateRecord() {
        Map<String, Object> record = new HashMap<>();

        // 生成主键
        String id;
        if (primaryKeyDuplicateRate > 0 && !generatedIdList.isEmpty() 
                && random.nextInt(100) < primaryKeyDuplicateRate) {
            // 生成重复主键(用于UPDATE场景)
            id = generatedIdList.get(random.nextInt(generatedIdList.size()));
        } else {
            // 生成新主键
            id = String.valueOf(idGenerator.getAndIncrement());
            if (generatedIds.add(id)) {
                generatedIdList.add(id);
            }
        }

        record.put("id", id);
        record.put("name", "name-" + id + "-" + System.currentTimeMillis());
        record.put("value", random.nextInt(1000000));
        record.put("ts", new Timestamp(System.currentTimeMillis()));

        return record;
    }

    /**
     * 生成批量记录事件
     */
    public List<TapRecordEvent> generateRecordEvents(int batchSize) {
        List<TapRecordEvent> events = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            Map<String, Object> record = generateRecord();
            TapInsertRecordEvent event = new TapInsertRecordEvent();
            event.setAfter(record);
            event.setTableId(tableName);
            event.setReferenceTime(System.currentTimeMillis());
            events.add(event);
        }

        return events;
    }

    /**
     * 生成更新事件
     */
    public TapUpdateRecordEvent generateUpdateEvent() {
        if (generatedIdList.isEmpty()) {
            return null;
        }

        String id = generatedIdList.get(random.nextInt(generatedIdList.size()));

        Map<String, Object> before = new HashMap<>();
        before.put("id", id);
        before.put("name", "name-" + id);
        before.put("value", random.nextInt(1000000));
        before.put("ts", new Timestamp(System.currentTimeMillis() - 86400000L));

        Map<String, Object> after = new HashMap<>();
        after.put("id", id);
        after.put("name", "name-" + id + "-updated-" + System.currentTimeMillis());
        after.put("value", random.nextInt(1000000));
        after.put("ts", new Timestamp(System.currentTimeMillis()));

        TapUpdateRecordEvent event = new TapUpdateRecordEvent();
        event.setBefore(before);
        event.setAfter(after);
        event.setTableId(tableName);
        event.setReferenceTime(System.currentTimeMillis());

        return event;
    }

    /**
     * 生成TapTable结构
     */
    public TapTable generateTapTable() {
        TapTable table = new TapTable();
        table.setName(tableName);
        table.setId(tableName);

        // 添加字段 - 使用正确的TapField构造函数
        TapField idField = new TapField();
        idField.setName("id");
        idField.setDataType("VARCHAR");
        idField.setNullable(true);
        idField.setPrimaryKey(true);
        idField.setPrimaryKeyPos(1);
        table.add(idField);

        TapField nameField = new TapField();
        nameField.setName("name");
        nameField.setDataType("VARCHAR");
        nameField.setNullable(true);
        table.add(nameField);

        TapField valueField = new TapField();
        valueField.setName("value");
        valueField.setDataType("INTEGER");
        valueField.setNullable(true);
        table.add(valueField);

        TapField tsField = new TapField();
        tsField.setName("ts");
        tsField.setDataType("TIMESTAMP");
        tsField.setNullable(true);
        table.add(tsField);

        // 设置主键
        table.setDefaultPrimaryKeys(Collections.singletonList("id"));

        return table;
    }

    /**
     * 生成指定数量的记录,按指定QPS控制速度
     * @param totalRecords 总记录数
     * @param qps 每秒写入记录数(0表示不限制)
     * @param consumer 处理批次数据的消费者
     */
    public void generateRecordsWithRate(long totalRecords, int qps, Consumer<List<TapRecordEvent>> consumer) {
        long batchSize = 1000; // 每批次1000条
        long totalBatches = (totalRecords + batchSize - 1) / batchSize;
        
        System.out.println("  >> 开始生成数据: 总量=" + totalRecords + "条, 批次大小=" + batchSize);
        if (qps > 0) {
            System.out.println("  >> QPS限制=" + qps + "条/秒");
        } else {
            System.out.println("  >> QPS限制=无限制(全速写入)");
        }

        long startTime = System.currentTimeMillis();
        long totalWritten = 0;

        for (long i = 0; i < totalBatches; i++) {
            long currentBatchSize = Math.min(batchSize, totalRecords - i * batchSize);

            // 生成批次数据
            List<TapRecordEvent> events = new ArrayList<>((int) currentBatchSize);
            for (int j = 0; j < currentBatchSize; j++) {
                Map<String, Object> record = generateRecord();
                TapInsertRecordEvent event = new TapInsertRecordEvent();
                event.setAfter(record);
                event.setTableId(tableName);
                event.setReferenceTime(System.currentTimeMillis());
                events.add(event);
            }

            // 调用消费者处理数据
            consumer.accept(events);
            totalWritten += events.size();

            // 控制QPS
            if (qps > 0) {
                long expectedTimePerBatch = (batchSize * 1000) / qps;
                long actualTime = System.currentTimeMillis() - startTime;
                long sleepTime = expectedTimePerBatch - actualTime;

                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                startTime = System.currentTimeMillis();
            }

            // 每10批次打印进度
            if ((i + 1) % 10 == 0 || i == totalBatches - 1) {
                double progress = (double) (i + 1) / totalBatches * 100;
                System.out.printf("  >> 进度: %d/%d 批次 (%.1f%%), 已写入: %d 条%n", 
                        i + 1, totalBatches, progress, totalWritten);
            }
        }

        System.out.println("  >> 数据生成完成: 总计 " + totalWritten + " 条");
    }

    /**
     * 获取已生成的唯一ID数量
     */
    public int getUniqueIdsCount() {
        return generatedIds.size();
    }

    /**
     * 获取总生成记录数(包括重复)
     */
    public long getTotalGenerated() {
        return idGenerator.get() - 1;
    }
}
